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
	deps   map[int32]yagpaxosproto.DepSet

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
		deps:   make(map[int32]yagpaxosproto.DepSet),

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
			go r.handlePropose(propose)
		case m := <-r.cs.fastAckChan:
			fastAck := m.(*yagpaxosproto.MFastAck)
			go r.handleFastAck(fastAck)
		case m := <-r.cs.commitChan:
			commit := m.(*yagpaxosproto.MCommit)
			go r.handleCommit(commit)
		case m := <-r.cs.slowAckChan:
			slowAck := m.(*yagpaxosproto.MSlowAck)
			go r.handleSlowAck(slowAck)
		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*yagpaxosproto.MNewLeader)
			go r.handleNewLeader(newLeader)
		case m := <-r.cs.newLeaderAckChan:
			newLeaderAck := m.(*yagpaxosproto.MNewLeaderAck)
			go r.handleNewLeaderAck(newLeaderAck)
		case m := <-r.cs.syncChan:
			sync := m.(*yagpaxosproto.MSync)
			go r.handleSync(sync)
		case m := <-r.cs.syncAckChan:
			syncAck := m.(*yagpaxosproto.MSyncAck)
			go r.handleSyncAck(syncAck)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER && r.status != FOLLOWER {
		return
	}

	p, exists := r.phases[msg.CommandId]
	if exists && p != START {
		return
	}

	r.deps[msg.CommandId] = yagpaxosproto.NewDepSet()
	for cid, p := range r.phases {
		if p != START && inConflict(r.cmds[cid], msg.Command) {
			r.deps[msg.CommandId].Add(cid)
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
	// for some strange architectural reason there is no way
	// to send a message to yourself
	// (see `PeerWriters` from `genericsmr.go`)
	go r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *yagpaxosproto.MFastAck) {
	log.Fatal("FastAck: nyr")
}

func (r *Replica) handleCommit(msg *yagpaxosproto.MCommit) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER && r.status != FOLLOWER {
		return
	}

	r.phases[msg.Instance] = COMMIT
	r.cmds[msg.Instance] = msg.Command
	r.deps[msg.Instance] = msg.Dep
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

type quorum struct {
	size     int
	elements []interface{}
}

type quorumSet struct {
	neededSize int
	quorums    map[int]*quorum
	related    func(interface{}, interface{}) bool
	out        chan []interface{}
}

func (qs *quorumSet) add(e interface{}) {
	if qs.neededSize < 1 {
		return
	}

	if len(qs.quorums) == 0 {
		qs.quorums = make(map[int]*quorum)
		qs.quorums[0] = &quorum{
			size:     1,
			elements: make([]interface{}, qs.neededSize),
		}
		qs.quorums[0].elements[0] = e

		if qs.neededSize < 2 {
			qs.out <- qs.quorums[0].elements
		}
		return
	}

	for _, q := range qs.quorums {
		if qs.related(q.elements[0], e) {
			q.elements[q.size] = e
			q.size++
			if q.size == qs.neededSize {
				qs.out <- q.elements
			}
			return
		}
	}

	qs.quorums[len(qs.quorums)] = &quorum{
		size:     1,
		elements: make([]interface{}, qs.neededSize),
	}
	qs.quorums[len(qs.quorums)-1].elements[0] = e
}
