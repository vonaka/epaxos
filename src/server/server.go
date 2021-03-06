package main

import (
	"bufio"
	"epaxos"
	"flag"
	"fmt"
	"genericsmr"
	"gpaxos"
	"log"
	"masterproto"
	"mencius"
	"net"
	"net/http"
	"net/rpc"
	"optpaxos"
	"os"
	"os/signal"
	"paxos"
	"runtime/pprof"
	"sim"
	"syscall"
	"time"
	"yagpaxos"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var doMencius *bool = flag.Bool("m", false, "Use Mencius as the replication protocol. Defaults to false.")
var doGpaxos *bool = flag.Bool("g", false, "Use Generalized Paxos as the replication protocol. Defaults to false.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doYagpaxos *bool = flag.Bool("y", false, "Use Yet Another GPaxos as the replication protocol. Defaults to false.")
var simYagpaxos *bool = flag.Bool("sy", false, "Simulate Yet Another GPaxos as the replication protocol. Defaults to false.")
var doOptpaxos *bool = flag.Bool("optpaxos", false, "Use optimized Paxos as the replication protocol. Defaults to false.")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", true, "Execute commands.")
var lread = flag.Bool("lread", false, "Execute locally read command.")
var dreply = flag.Bool("dreply", true, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var maxfailures = flag.Int("maxfailures", -1, "maximum number of maxfailures; default is a minority, ignored by other protocols than Paxos.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var batchWait *int = flag.Int("batchwait", 0, "Milliseconds to wait before sending a batch. If set to 0, batching is disabled. Defaults to 0.")
var transitiveConflicts *bool = flag.Bool("transitiveconf", true, "Conflict relation is transitive.")
var proxy = flag.String("proxy", "NONE", "List of proxy IPs for this server")
var qfile *string = flag.String("qfile", "", "Quorum config file (for yagpaxos only).")
var descNum *int = flag.Int("desc", 100, "Number of command descriptors (only for yagpaxos and optpaxos).")
var poolLevel *int = flag.Int("pool", 1, "Level of pool usage from 0 to 2 (only for yagpaxos and optpaxos).")
var conf *int = flag.Int("conflict", 100, "Conflict rate (used in simulations).")

func initProxy(proxy string) {
	if proxy == "NONE" {
		return
	}

	f, err := os.Open(proxy)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		genericsmr.ProxyAddrs[s.Text()] = struct{}{}
	}

}

func main() {
	flag.Parse()

	initProxy(*proxy)

	if *doMencius && *thrifty {
		log.Fatal("incompatble options -m -thrifty")
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGUSR1)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList, isLeader := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))

	if *doEpaxos || *doMencius || *doGpaxos || *maxfailures == -1 {
		*maxfailures = (len(nodeList) - 1) / 2
	}

	log.Printf("Tolerating %d max. failures\n", *maxfailures)

	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *beacon, *durable, *batchWait, *transitiveConflicts, *maxfailures)
		rpc.Register(rep)
	} else if *doMencius {
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(replicaId, nodeList, *thrifty, *exec, *lread, *dreply, *durable, *maxfailures)
		rpc.Register(rep)
	} else if *doGpaxos {
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *maxfailures)
		rpc.Register(rep)
	} else if *doYagpaxos {
		log.Println("Starting Yet Another Generalized Paxos replica...")
		yagpaxos.MaxDescRoutines = *descNum
		rep := yagpaxos.NewReplica(replicaId, nodeList, *exec, *dreply,
			*poolLevel, *maxfailures, *qfile)
		rpc.Register(rep)
	} else if *simYagpaxos {
		log.Println("Starting Yet Another Generalized Paxos replica...")
		yagpaxos.MaxDescRoutines = *descNum
		rep := sim.NewReplica(replicaId, nodeList, *maxfailures, *qfile, *conf)
		rpc.Register(rep)
	} else if *doOptpaxos {
		log.Println("Starting optimized Paxos replica...")
		yagpaxos.MaxDescRoutines = *descNum
		rep := optpaxos.NewReplica(replicaId, nodeList, *thrifty, *exec,
			*lread, *dreply, *maxfailures, *qfile)
		rpc.Register(rep)
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, isLeader, *thrifty, *exec, *lread, *dreply, *durable, *batchWait, *maxfailures)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)

}

func registerWithMaster(masterAddr string) (int, []string, bool) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
	var reply masterproto.RegisterReply

	log.Printf("connecting to: %v", masterAddr)
	for {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			for {
				// This is an active wait, not cool.
				err = mcli.Call("Master.Register", args, &reply)
				if err == nil {
					if reply.Ready {
						break
					}
					time.Sleep(4)
				} else {
					log.Printf("%v", err)
				}
			}
			break
		} else {
			log.Printf("%v", err)
		}
		time.Sleep(4)
	}

	return reply.ReplicaId, reply.NodeList, reply.IsLeader
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("profing")
	//os.Exit(0)
}
