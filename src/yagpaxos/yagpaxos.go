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

	phases map[int32]phase
	cmds   map[int32]state.Command
	deps   map[int32](map[int32]bool)

	cs CommunicationSupply
}

type status int
type phase int

const (
	LEADER status = iota
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

		status:  FOLLOWER,
		ballot:  0,
		cballot: 0,

		phases: make(map[int32]phase),
		cmds:   make(map[int32]state.Command),
		deps:   make(map[int32](map[int32]bool)),

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
	if r.status == PREPARING {
		return
	}

	p, exists := r.phases[msg.CommandId]
	if exists && p != START {
		return
	}

	r.deps[msg.CommandId] = make(map[int32]bool)
	for cid, p := range r.phases {
		if p != START && inConflict(r.cmds[cid], msg.Command) {
			r.deps[msg.CommandId][cid] = true
		}
	}
	r.phases[msg.CommandId] = FAST_ACCEPT
	r.cmds[msg.CommandId] = msg.Command

	fastAck := &yagpaxosproto.MFastAck{
		Replica:  r.Id,
		Ballot:   r.ballot,
		Instance: msg.CommandId,
		// if I'm not mistaken,
		// there is no need to copy these two maps:
		Dep: r.deps[msg.CommandId],
	}
	r.sendToAll(fastAck, r.cs.fastAckRPC)
}

func (r *Replica) handleFastAck(msg *yagpaxosproto.MFastAck) {
	log.Fatal("FastAck: nyr")
}

func (r *Replica) handleCommit(msg *yagpaxosproto.MCommit) {
	log.Fatal("Commit: nyr")
}

func (r *Replica) handleSlowAck(msg *yagpaxosproto.MSlowAck) {
	log.Fatal("SlowAck: nyr")
}

func (r *Replica) handleNewLeader(msg *yagpaxosproto.MNewLeader) {
	log.Fatal("NewLeader: nyr")
}

func (r *Replica) handleNewLeaderAck(msg *yagpaxosproto.MNewLeaderAck) {
	log.Fatal("NewLeaderAck: nyr")
}

func (r *Replica) handleSync(msg *yagpaxosproto.MSync) {
	log.Fatal("Sync: nyr")
}

func (r *Replica) handleSyncAck(msg *yagpaxosproto.MSyncAck) {
	log.Fatal("SyncAck: nyr")
}

func (r *Replica) sendToAll(msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		if r.Alive[p] {
			r.SendMsg(p, rpc, msg)
		}
	}
}

func inConflict(c1 state.Command, c2 state.Command) bool {
	return false
}
