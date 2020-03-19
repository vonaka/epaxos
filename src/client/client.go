package main

import (
	"bindings"
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"state"
	"strings"
	"time"

	"github.com/google/uuid"
)

var clientId string = *flag.String("id", "", "the id of the client. Default is RFC 4122 nodeID.")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port. ")
var reqsNb *int = flag.Int("q", 1000, "Total number of requests. ")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). ")
var psize *int = flag.Int("psize", 100, "Payload size for writes.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). ")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. ")
var localReads *bool = flag.Bool("l", false, "Execute reads at the closest (local) replica. ")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. ")
var conflicts *int = flag.Int("c", 0, "Percentage of conflicts. Defaults to 0%")
var verbose *bool = flag.Bool("v", false, "verbose mode. ")
var scan *bool = flag.Bool("s", false, "replace read with short scan (100 elements)")
var latency *string = flag.String("delay", "0", "Node latency (in ms).")
var collocatedWith *string = flag.String("server", "NONE", "Server with which this client is collocated")
var lfile *string = flag.String("lfile", "NONE", "Latency file.")
var almostFast *bool = flag.Bool("af", false, "Almost fast (send propose to the leader and collocated server, needed for optimized Paxos).")
var myAddr *string = flag.String("addr", "", "Client address (this machine). Defaults to localhost.")

func updateLatencies(filename string) {
	if filename == "NONE" {
		if *collocatedWith != "NONE" {
			zero, _ := time.ParseDuration("0ms")
			addrs, _ := net.LookupIP(*collocatedWith)
			for _, addr := range addrs {
				bindings.AddrLatency[addr.String()] = zero
			}
		}
		return
	}

	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		d := strings.Split(s.Text(), ",")
		if len(d) != 3 {
			log.Fatal(filename + ": Wrong file format")
		}

		delay, _ := time.ParseDuration(d[2] + "ms")

		if *myAddr == d[0] {
			bindings.AddrLatency[d[1]] = delay
			addrs, _ := net.LookupIP(d[1])
			for _, addr := range addrs {
				bindings.AddrLatency[addr.String()] = delay
			}
		} else if *myAddr == d[1] {
			bindings.AddrLatency[d[0]] = delay
			addrs, _ := net.LookupIP(d[0])
			for _, addr := range addrs {
				bindings.AddrLatency[addr.String()] = delay
			}
		}
	}

	err = s.Err()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	flag.Parse()

	updateLatencies(*lfile)

	runtime.GOMAXPROCS(*procs)

	rand.Seed(time.Now().UnixNano())

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	bindings.Latency, _ = time.ParseDuration(*latency + "ms")
	bindings.CollocatedWith = *collocatedWith

	var proxy *bindings.Parameters
	for {
		proxy = bindings.NewParameters(*masterAddr, *masterPort, *verbose, *noLeader, *fast, *almostFast, *localReads)
		err := proxy.Connect()
		if err == nil {
			break
		}
		proxy.Disconnect()
	}

	if clientId == "" {
		clientId = uuid.New().String()
	}

	log.Printf("client: %v (verbose=%v, psize=%v, conflicts=%v)", clientId, *verbose, *psize, *conflicts)

	karray := make([]state.Key, *reqsNb)
	put := make([]bool, *reqsNb)

	clientKey := state.Key(uint64(uuid.New().Time())) // a command id unique to this client.
	for i := 0; i < *reqsNb; i++ {
		put[i] = false
		if *writes > 0 {
			r := rand.Intn(100)
			if r <= *writes {
				put[i] = true
			}
		}
		karray[i] = clientKey
		if *conflicts > 0 {
			r := rand.Intn(100)
			if r <= *conflicts {
				karray[i] = 42
			}
		}
	}

	var before_total time.Time

	// send one more request to be sure that at the moment when
	// the remaining requests are send, all servers are ready to accept them
	for j := 0; j < *reqsNb+1; j++ {

		if j == 1 {
			before_total = time.Now()
		}

		before := time.Now()

		real_j := func() int {
			if j != 0 {
				return j - 1
			}
			return j
		}()

		key := int64(karray[real_j])

		if put[real_j] {
			value := make([]byte, *psize)
			rand.Read(value)
			proxy.Write(key, state.Value(value))
		} else {
			if *scan {
				proxy.Scan(key, int64(100))
			} else {
				proxy.Read(key)
			}
		}

		after := time.Now()

		if j != 0 {
			duration := after.Sub(before)
			fmt.Printf("latency %v\n", to_ms(duration.Nanoseconds()))
			fmt.Printf("chain %d-1\n", int64(to_ms(after.UnixNano())))
		}
	}

	// FIXME: with `f` option `proxy.Stats()` might block
	// fmt.Printf(proxy.Stats() + "\n")

	after_total := time.Now()
	fmt.Printf("Test took %v\n", after_total.Sub(before_total))

	proxy.Disconnect()
}

// convert nanosecond to millisecond
func to_ms(nano int64) float64 {
	return float64(nano) / float64(time.Millisecond)
}
