package bindings

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"genericsmrproto"
	"io"
	"log"
	"masterproto"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"state"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const TRUE = uint8(1)
const TIMEOUT = 3 * time.Second
const MAX_ATTEMPTS = 3

type Parameters struct {
	clientId       int32
	masterAddr     string
	masterPort     int
	verbose        bool
	localReads     bool
	closestReplica int
	Leader         int
	leaderless     bool
	isFast         bool
	isAlmostFast   bool
	n              int
	replicaLists   []string
	servers        []net.Conn
	readers        []*bufio.Reader
	writers        []*bufio.Writer
	id             int32
	retries        int32

	repChan  chan *fastRep
	maxCmdId int32

	collocatedWith string
	collocatedId   int
	latency        time.Duration
	idLatency      map[int]time.Duration
	addrLatency    map[string]time.Duration

	logger *log.Logger
}

type fastRep struct {
	rep *genericsmrproto.ProposeReplyTS
	id  int
}

var (
	CollocatedWith = "NONE"
	Latency        = time.Duration(0)
	AddrLatency    = make(map[string]time.Duration)
)

func getClinetId() int32 {
	return int32(uuid.New().ID())
}

func NewParameters(masterAddr string, masterPort int,
	verbose, leaderless, fast, almostFast, localReads bool,
	logger *log.Logger) *Parameters {

	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	p := &Parameters{
		clientId:       getClinetId(),
		masterAddr:     masterAddr,
		masterPort:     masterPort,
		verbose:        verbose,
		localReads:     localReads,
		closestReplica: 0,
		Leader:         0,
		leaderless:     leaderless,
		isFast:         fast,
		isAlmostFast:   almostFast,
		n:              0,
		replicaLists:   nil,
		servers:        nil,
		readers:        nil,
		writers:        nil,
		id:             0,
		retries:        10,

		maxCmdId: 0,

		collocatedWith: CollocatedWith,
		collocatedId:   -1,
		latency:        Latency,
		idLatency:      make(map[int]time.Duration),
		addrLatency:    make(map[string]time.Duration),

		logger: logger,
	}

	for k,v := range AddrLatency {
		p.addrLatency[k] = v
	}

	return p
}

func (b *Parameters) Id() int32 {
	return b.clientId
}

func (b *Parameters) Connect() error {
	var err error

	b.logger.Printf("Dialing master...\n")
	var master *rpc.Client
	master = b.MasterDial()

	b.logger.Printf("Getting replica list from master...\n")
	var replyRL *masterproto.GetReplicaListReply

	// loop until the call succeeds
	// (or too many attempts)
	for i, done := 0, false; !done; i++ {
		replyRL = new(masterproto.GetReplicaListReply)
		err = Call(master, "Master.GetReplicaList",
			new(masterproto.GetReplicaListArgs), replyRL, b.logger)
		if err == nil && replyRL.Ready {
			done = true
		} else if i == MAX_ATTEMPTS {
			// if too many attempts, connect again
			master.Close()
			return errors.New("Too many call attempts!")
		}
	}

	// get closest replica
	err = b.FindClosestReplica(replyRL)
	if err != nil {
		return err
	}
	b.logger.Printf("node list %v, closest (alive) = %v", b.replicaLists, b.closestReplica)

	// init some parameters
	b.n = len(b.replicaLists)
	b.servers = make([]net.Conn, b.n)
	b.readers = make([]*bufio.Reader, b.n)
	b.writers = make([]*bufio.Writer, b.n)
	b.repChan = make(chan *fastRep, b.n)

	// get list of nodes to connect to
	var toConnect []int
	if b.isFast {
		for i := 0; i < b.n; i++ {
			if replyRL.AliveList[i] {
				toConnect = append(toConnect, i)
			}
		}
	} else {
		if !b.leaderless {
			b.logger.Printf("Getting leader from master...\n")
			var replyL *masterproto.GetLeaderReply

			for i, done := 0, false; !done; i++ {
				replyL = new(masterproto.GetLeaderReply)
				err = Call(master, "Master.GetLeader",
					new(masterproto.GetLeaderArgs), replyL, b.logger)
				if err == nil {
					done = true
				} else if i == MAX_ATTEMPTS {
					// if too many attempts, connect again
					master.Close()
					return errors.New("Too many call attempts!")
				}
			}

			b.Leader = replyL.LeaderId
			if b.closestReplica != b.Leader {
				toConnect = append(toConnect, b.Leader)
			}
			b.logger.Printf("The Leader is replica %d\n", b.Leader)
		}
		if b.isAlmostFast {
			toConnect = append(toConnect, b.collocatedId)
		} else {
			toConnect = append(toConnect, b.closestReplica)
		}
	}

	for _, i := range toConnect {
		b.logger.Println("Connection to ", i, " -> ", b.replicaLists[i])
		b.servers[i] = Dial(b.replicaLists[i], false, b.logger)
		b.readers[i] = bufio.NewReader(b.servers[i])
		b.writers[i] = bufio.NewWriter(b.servers[i])
		/*if b.isFast || b.isAlmostFast {
			go func(delay time.Duration, rep int) {
				reply := new(genericsmrproto.ProposeReplyTS)
				for {
					reply.Unmarshal(b.readers[rep])
					time.Sleep(delay)
					b.repChan <- &fastRep{
						rep: reply,
						id:  rep,
					}
					// TODO handle errors
				}
			}(IdLatency[i], i)
		}*/
	}

	b.logger.Println("Connected")

	return nil
}

func Dial(addr string, connect bool, logger *log.Logger) net.Conn {
	var conn net.Conn
	var err error
	var resp *http.Response

	for try := 0; try < 3; try++ {
		conn, err = net.DialTimeout("tcp", addr, TIMEOUT)

		if err == nil {
			if connect {
				// connect if no error
				io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
				resp, err = http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
				if err == nil && resp != nil && resp.Status == "200 Connected to Go RPC" {
					break
				}
			} else {
				break
			}
		}

		if try == 2 {
			// if not done yet, try again
			logger.Println("Connection error with ", addr, ": ", err)
			if conn != nil {
				conn.Close()
			}
			logger.Fatal("Will not try anymore")
		}
	}

	return conn
}

func (b *Parameters) MasterDial() *rpc.Client {
	var master *rpc.Client
	var addr string
	var conn net.Conn

	addr = fmt.Sprintf("%s:%d", b.masterAddr, b.masterPort)
	conn = Dial(addr, true, b.logger)
	master = rpc.NewClient(conn)

	return master
}

func Call(cli *rpc.Client, method string,
	args interface{}, reply interface{}, logger *log.Logger) error {

	c := make(chan error, 1)
	go func() { c <- cli.Call(method, args, reply) }()
	select {
	case err := <-c:
		if err != nil {
			logger.Printf("Error in RPC: " + method)
		}
		return err

	case <-time.After(TIMEOUT):
		logger.Printf("RPC timeout: " + method)
		return errors.New("RPC timeout")
	}
}

func (p *Parameters) getDelay(addr string) time.Duration {
	d, exists := p.addrLatency[addr]
	if exists {
		return d
	}

	if addr != p.collocatedWith {
		return p.latency
	}

	d, _ = time.ParseDuration("0ms")
	return d
}

func (b *Parameters) FindClosestReplica(replyRL *masterproto.GetReplicaListReply) error {
	// save replica list and closest
	b.replicaLists = replyRL.ReplicaList

	b.logger.Printf("Pinging all replicas...\n")

	found := false
	minLatency := math.MaxFloat64
	for i := 0; i < len(b.replicaLists); i++ {
		if !replyRL.AliveList[i] {
			continue
		}
		addr := strings.Split(string(b.replicaLists[i]), ":")[0]
		if addr == "" {
			addr = "127.0.0.1"
		}

		b.idLatency[i] = b.getDelay(addr)

		if addr == b.collocatedWith || b.idLatency[i] == time.Duration(0) {
			found = true
			b.collocatedId = i
			b.closestReplica = i
		}

		out, err := exec.Command("ping", addr, "-c 3", "-q").Output()
		if err == nil {
			// parse ping output
			latency, _ := strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			b.logger.Printf("%v -> %v", i, latency)

			// save if closest replica
			if minLatency > latency && !found {
				b.closestReplica = i
				minLatency = latency
			}
		} else {
			b.logger.Printf("cannot ping " + b.replicaLists[i])
			return err
		}
	}

	return nil
}

func (b *Parameters) Disconnect() {
	for _, server := range b.servers {
		if server != nil {
			server.Close()
		}
	}
	b.logger.Printf("Disconnected")
}

// not idempotent in case of a failure
func (b *Parameters) Write(key int64, value []byte) {
	b.id++
	args := genericsmrproto.Propose{
		CommandId: b.id,
		ClientId:  b.clientId,
		Command:   state.Command{state.PUT, state.Key(key), value},
		Timestamp: 0,
	}

	if b.verbose {
		b.logger.Println(args.Command.String())
	}

	b.execute(args)
}

func (b *Parameters) Read(key int64) []byte {
	b.id++
	args := genericsmrproto.Propose{
		CommandId: b.id,
		ClientId:  b.clientId,
		Command:   state.Command{state.GET, state.Key(key), state.NIL()},
		Timestamp: 0,
	}

	if b.verbose {
		b.logger.Println(args.Command.String())
	}

	return b.execute(args)
}

func (b *Parameters) Scan(key int64, count int64) []byte {
	b.id++
	args := genericsmrproto.Propose{
		CommandId: b.id,
		ClientId:  b.clientId,
		Command:   state.Command{state.SCAN, state.Key(key), make([]byte, 8)},
		Timestamp: 0,
	}

	binary.LittleEndian.PutUint64(args.Command.V, uint64(count))

	if b.verbose {
		b.logger.Println(args.Command.String())
	}

	return b.execute(args)
}

func (b *Parameters) Stats() string {
	b.writers[b.closestReplica].WriteByte(genericsmrproto.STATS)
	b.writers[b.closestReplica].Flush()
	arr := make([]byte, 1000)
	b.readers[b.closestReplica].Read(arr)
	return string(bytes.Trim(arr, "\x00"))
}

// internals

func (b *Parameters) execute(args genericsmrproto.Propose) []byte {
	err := errors.New("")
	var value state.Value

	submitter := b.Leader
	if b.leaderless {
		submitter = b.closestReplica
	}

	for err != nil {

		if !b.isFast && !b.isAlmostFast {
			b.writers[submitter].WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(b.writers[submitter])
			b.writers[submitter].Flush()
			if b.verbose {
				b.logger.Println("Sent to ", submitter)
			}
		} else {
			if b.verbose {
				if b.isFast {
					b.logger.Println("Sent to everyone", args.CommandId)
				} else {
					b.logger.Println("Sned to leader and collocated server",
						args.CommandId)
				}
			}
			for rep := 0; rep < b.n; rep++ {
				if b.writers[rep] != nil {
					b.writers[rep].WriteByte(genericsmrproto.PROPOSE)
					args.Marshal(b.writers[rep])
					b.writers[rep].Flush()
				}
			}
		}

		if b.isFast || b.isAlmostFast {
			value, err = b.waitReplies(b.collocatedId, args.CommandId)
			//value, err = b.fastWaitReplies(args.CommandId)
		} else {
			value, err = b.waitReplies(submitter, args.CommandId)
		}

		if err != nil {

			b.logger.Println("Error: ", err)

			for err != nil && b.retries > 0 {
				b.retries--
				b.Disconnect()
				b.logger.Println("Reconnecting ...")
				time.Sleep(TIMEOUT) // must be inline with the closest quorum re-computation
				err = b.Connect()
			}

			if err != nil && b.retries == 0 {
				b.logger.Fatal("Cannot recover.")
			}

		}

	}

	if b.verbose {
		b.logger.Println("Returning: ", value.String())
	}

	return value
}

func (b *Parameters) fastWaitReplies(cmdId int32) (state.Value, error) {
	for {
		fr := <-b.repChan
		if fr.rep.CommandId == cmdId {
			if fr.rep.OK == TRUE {
				return fr.rep.Value, nil
			} else {
				return state.NIL(), errors.New("Failed to receive a response.")
			}
		}
	}

	return nil, nil
}

func (b *Parameters) waitReplies(submitter int,
	cmdId int32) (state.Value, error) {
	var err error
	ret := state.NIL()

	rep := new(genericsmrproto.ProposeReplyTS)
	for {
		if err = rep.Unmarshal(b.readers[submitter]); err == nil {
			if rep.CommandId != cmdId {
				continue
			}
			if rep.OK == TRUE {
				ret = rep.Value
				break
			} else {
				err = errors.New("Failed to receive a response.")
				break
			}
		}
	}

	time.Sleep(b.idLatency[submitter])
	return ret, err
}
