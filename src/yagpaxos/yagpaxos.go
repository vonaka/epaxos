package yagpaxos

import (
	"fastrpc"
	"genericsmr"
	"log"
	"state"
	"sync"
	"yagpaxosproto"
)

type Replica struct {
	*genericsmr.Replica
	*sync.Mutex

	status  status
	ballot  int32
	cballot int32

	phase map[state.Id]phase
	cmds  map[state.Id]state.Command
	deps  map[state.Id](map[state.Id]bool)

	cs CommunicationSupply
}

type phase int
type status int

const (
	LEADER phase = iota
	FOLLOWER
	PREPARING
)

const (
	START phase = iota
	FAST_ACCEPT
	SLOW_ACCEPT
	COMMIT
	DELIVER
)

type CommunicationSupply struct {
	//fastAcceptChan   chan fastrpc.Serializable
	fastAckChan      chan fastrpc.Serializable
	commitChan       chan fastrpc.Serializable
	slowAckChan      chan fastrpc.Serializable
	newLeaderChan    chan fastrpc.Serializable
	newLeaderAckChan chan fastrpc.Serializable
	syncChan         chan fastrpc.Serializable
	syncAckChan      chan fastrpc.Serializable

	//fastAcceptRPC    uint8
	fastAckRPC      uint8
	commitRPC       uint8
	slowAckRPC      uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	syncRPC         uint8
	syncAckRPC      uint8
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty bool, exec bool, lread bool, dreply bool) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply),

		status:  status(FOLLOWER),
		ballot:  0,
		cballot: 0,

		phase: make(map[state.Id]phase),
		cmds:  make(map[state.Id]state.Command),
		deps:  make(map[state.Id](map[state.Id]bool)),

		cs: CommunicationSupply{
			//fastAcceptChan:   make(chan fastrpc.Serializable,
			//	genericsmr.CHAN_BUFFER_SIZE),
			fastAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			commitChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			slowAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			newLeaderChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			newLeaderAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			syncChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			syncAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
		},
	}

	//r.fastAcceptRPC = r.RegisterRPC()
	r.cs.fastAckRPC =
		r.RegisterRPC(new(yagpaxosproto.MFastAck), r.cs.fastAckChan)
	r.cs.commitRPC =
		r.RegisterRPC(new(yagpaxosproto.MCommit), r.cs.commitChan)
	r.cs.slowAckRPC =
		r.RegisterRPC(new(yagpaxosproto.MSlowAck), r.cs.slowAckChan)
	r.cs.newLeaderRPC =
		r.RegisterRPC(new(yagpaxosproto.MNewLeader), r.cs.newLeaderChan)
	r.cs.newLeaderAckRPC =
		r.RegisterRPC(new(yagpaxosproto.MNewLeaderAck), r.cs.newLeaderAckChan)
	r.cs.syncRPC =
		r.RegisterRPC(new(yagpaxosproto.MSync), r.cs.syncChan)
	r.cs.syncAckRPC =
		r.RegisterRPC(new(yagpaxosproto.MSyncAck), r.cs.syncAckChan)

	go r.run()

	return &r
}

func (r *Replica) run() {
	go func() {
		r.ConnectToPeers()
		r.ComputeClosestPeers()
		r.WaitForClientConnections()
	}()

	for !r.Shutdown {
		select {
		case propose := <-r.ProposeChan:
			r.handlePropose(propose)
		case m := <-r.cs.fastAckChan:
			fastAck := m.(*yagpaxosproto.MFastAck)
			r.handleFastAck(fastAck)
		case m := <-r.cs.commitChan:
			commit := m.(*yagpaxosproto.MCommit)
			r.handleCommit(commit)
		case m := <-r.cs.slowAckChan:
			slowAck := m.(*yagpaxosproto.MSlowAck)
			r.handleSlowAck(slowAck)
		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*yagpaxosproto.MNewLeader)
			r.handleNewLeader(newLeader)
		case m := <-r.cs.newLeaderAckChan:
			newLeaderAck := m.(*yagpaxosproto.MNewLeaderAck)
			r.handleNewLeaderAck(newLeaderAck)
		case m := <-r.cs.syncChan:
			sync := m.(*yagpaxosproto.MSync)
			r.handleSync(sync)
		case m := <-r.cs.syncAckChan:
			syncAck := m.(*yagpaxosproto.MSyncAck)
			r.handleSyncAck(syncAck)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	log.Fatal("nyr")
}

func (r *Replica) handleFastAck(msg *yagpaxosproto.MFastAck) {
	log.Fatal("nyr")
}

func (r *Replica) handleCommit(msg *yagpaxosproto.MCommit) {
	log.Fatal("nyr")
}

func (r *Replica) handleSlowAck(msg *yagpaxosproto.MSlowAck) {
	log.Fatal("nyr")
}

func (r *Replica) handleNewLeader(msg *yagpaxosproto.MNewLeader) {
	log.Fatal("nyr")
}

func (r *Replica) handleNewLeaderAck(msg *yagpaxosproto.MNewLeaderAck) {
	log.Fatal("nyr")
}

func (r *Replica) handleSync(msg *yagpaxosproto.MSync) {
	log.Fatal("nyr")
}

func (r *Replica) handleSyncAck(msg *yagpaxosproto.MSyncAck) {
	log.Fatal("nyr")
}
