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
	"os/exec"
	"state"
	"strconv"
	"strings"
	"time"
)

const TRUE = uint8(1)
const TIMEOUT = 5 * time.Second

type Parameters struct {
	masterAddr     string
	masterPort     int
	verbose        bool
	localReads     bool
	closestReplica int
	Leader         int
	leaderless     bool
	isFast         bool
	n              int
	replicaLists   []string
	servers        []net.Conn
	readers        []*bufio.Reader
	writers        []*bufio.Writer
	id             int32
	retries        int32

	repChan chan *genericsmrproto.ProposeReplyTS
}

func NewParameters(masterAddr string, masterPort int, verbose bool, leaderless bool, fast bool, localReads bool) *Parameters {
	return &Parameters{
		masterAddr:     masterAddr,
		masterPort:     masterPort,
		verbose:        verbose,
		localReads:     localReads,
		closestReplica: 0,
		Leader:         0,
		leaderless:     leaderless,
		isFast:         fast,
		n:              0,
		replicaLists:   nil,
		servers:        nil,
		readers:        nil,
		writers:        nil,
		id:             0,
		retries:        10,
	}
}

func (b *Parameters) Connect() error {
	var resp *http.Response

	log.Printf("Dialing master...\n")
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", b.masterAddr, b.masterPort), TIMEOUT)
	if err == nil {
		io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")
		resp, err = http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	}

	if err != nil || resp == nil || resp.Status != "200 Connected to Go RPC" {
		log.Printf("Error connecting to master\n")
		conn.Close()
		return err
	}

	master := rpc.NewClient(conn)
	log.Printf("Getting replica list from master...\n")
	var rlReply *masterproto.GetReplicaListReply
	for done := false; !done; {
		rlReply = new(masterproto.GetReplicaListReply)
		err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
		if err != nil {
			log.Printf("Error making the GetReplicaList RPC")
			master.Close()
			return err
		}
		if rlReply.Ready {
			done = true
		}
	}

	log.Printf("Pinging all replicas...\n")
	minLatency := math.MaxFloat64
	b.replicaLists = rlReply.ReplicaList
	for i := 0; i < len(b.replicaLists); i++ {
		if !rlReply.AliveList[i] {
			continue
		}
		addr := strings.Split(string(b.replicaLists[i]), ":")[0]
		if addr == "" {
			addr = "127.0.0.1"
		}
		out, err := exec.Command("ping", addr, "-c 3", "-q").Output()
		if err == nil {
			latency, _ := strconv.ParseFloat(strings.Split(string(out), "/")[4], 64)
			log.Printf("%v -> %v", i, latency)
			if minLatency > latency {
				b.closestReplica = i
				minLatency = latency
			}
		} else {
			log.Printf("cannot connect to " + b.replicaLists[i])
			return err
		}
	}

	log.Printf("node list %v, closest (alive) = (%v,%vms)", b.replicaLists, b.closestReplica, minLatency)

	b.n = len(b.replicaLists)

	b.servers = make([]net.Conn, b.n)
	b.readers = make([]*bufio.Reader, b.n)
	b.writers = make([]*bufio.Writer, b.n)

	var toConnect []int

	if b.isFast {
		for i := 0; i < b.n; i++ {
			toConnect = append(toConnect, i)
		}
	} else {
		toConnect = append(toConnect, b.closestReplica)
	}
	if !b.leaderless {
		reply := new(masterproto.GetLeaderReply)
		if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
			log.Printf("Error making the GetLeader RPC\n")
			master.Close()
			return err
		}
		b.Leader = reply.LeaderId
		if b.closestReplica != b.Leader {
			if !b.isFast {
				toConnect = append(toConnect, b.Leader)
			}
		}
		log.Printf("The Leader is replica %d\n", b.Leader)
	}

	b.repChan = make(chan *genericsmrproto.ProposeReplyTS, b.n)

	for _, i := range toConnect {
		log.Println("Connection to ", i, " -> ", b.replicaLists[i])
		b.servers[i], err = net.DialTimeout("tcp", b.replicaLists[i], 10*time.Second)
		if err != nil {
			log.Println("Connection error with ", b.replicaLists[i])
			return err
		} else {
			b.readers[i] = bufio.NewReader(b.servers[i])
			b.writers[i] = bufio.NewWriter(b.servers[i])

			if b.isFast {
				go func(rep int) {
					for {
						reply := new(genericsmrproto.ProposeReplyTS)
						reply.Unmarshal(b.readers[rep])
						b.repChan <- reply
						// TODO handle errors
					}
				}(i)
			}
		}
	}

	log.Println("Connected")

	return nil

}

func (b *Parameters) Disconnect() {
	for _, server := range b.servers {
		if server != nil {
			server.Close()
		}
	}
	log.Printf("Disconnected")
}

// not idempotent in case of a failure
func (b *Parameters) Write(key int64, value []byte) {
	b.id++
	args := genericsmrproto.Propose{b.id,
		state.Command{state.PUT, state.Key(key), value}, 0}

	if b.verbose {
		log.Println(args.Command.String())
	}

	b.execute(args)
}

func (b *Parameters) Read(key int64) []byte {
	b.id++
	args := genericsmrproto.Propose{b.id,
		state.Command{state.GET, state.Key(key), state.NIL()}, 0}

	if b.verbose {
		log.Println(args.Command.String())
	}

	return b.execute(args)
}

func (b *Parameters) Scan(key int64, count int64) []byte {
	b.id++
	args := genericsmrproto.Propose{b.id,
		state.Command{state.SCAN, state.Key(key), make([]byte, 8)}, 0}

	binary.LittleEndian.PutUint64(args.Command.V, uint64(count))

	if b.verbose {
		log.Println(args.Command.String())
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

	for err != nil {

		submitter := b.Leader
		if b.leaderless || ((args.Command.Op == state.GET || args.Command.Op == state.SCAN) && b.localReads) {
			submitter = b.closestReplica
		}

		if !b.isFast {
			b.writers[submitter].WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(b.writers[submitter])
			b.writers[submitter].Flush()
			if b.verbose {
				log.Println("Sent to ", submitter)
			}
		} else {
			if b.verbose {
				log.Println("Sent to everyone")
			}
			for rep := 0; rep < b.n; rep++ {
				b.writers[rep].WriteByte(genericsmrproto.PROPOSE)
				args.Marshal(b.writers[rep])
				b.writers[rep].Flush()
			}
		}

		if b.isFast {
			value, err = b.fastWaitReplies(args.CommandId)
		} else {
			value, err = b.waitReplies(submitter, args.CommandId)
		}

		if err != nil {

			log.Println("Error: ", err)

			for err != nil && b.retries > 0 {
				b.retries--
				b.Disconnect()
				log.Println("Reconnecting ...")
				time.Sleep(TIMEOUT) // must be inline with the closest quorum re-computation
				err = b.Connect()
			}

			if err != nil && b.retries == 0 {
				log.Fatal("Cannot recover.")
			}

		}

	}

	if b.verbose {
		log.Println("Returning: ", value.String())
	}

	return value
}

func (b *Parameters) fastWaitReplies(cmdId int32) (state.Value, error) {
	for {
		rep := <-b.repChan
		if rep.CommandId == cmdId {
			if rep.OK == TRUE {
				ret := rep.Value
				return ret, nil
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

	return ret, err
}
