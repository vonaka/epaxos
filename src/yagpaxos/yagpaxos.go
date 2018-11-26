package yagpaxos

import (
	"errors"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"log"
	"sort"
	"state"
	"sync"
	"time"
	"yagpaxosproto"
)

type Replica struct {
	*genericsmr.Replica
	sync.Mutex

	status  status
	ballot  int32
	cballot int32

	phases map[int32]phase
	cmds   map[int32]state.Command
	deps   map[int32]yagpaxosproto.DepSet

	slowAckQuorumSets map[int32]*quorumSet
	fastAckQuorumSets map[int32]*quorumSet

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
	fastAckChan      chan fastrpc.Serializable
	commitChan       chan fastrpc.Serializable
	slowAckChan      chan fastrpc.Serializable
	newLeaderChan    chan fastrpc.Serializable
	newLeaderAckChan chan fastrpc.Serializable
	syncChan         chan fastrpc.Serializable
	syncAckChan      chan fastrpc.Serializable

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

		slowAckQuorumSets: make(map[int32]*quorumSet),
		fastAckQuorumSets: make(map[int32]*quorumSet),

		cs: CommunicationSupply{
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
		Command:  msg.Command,
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
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) ||
		r.ballot != msg.Ballot {
		return
	}

	fastQuorumSize := 3*r.N/4 + 1
	slowQuorumSize := r.N/2 + 1

	qs, exists := r.fastAckQuorumSets[msg.Instance]
	if !exists {
		related := func(e1 interface{}, e2 interface{}) bool {
			fastAck1 := e1.(*yagpaxosproto.MFastAck)
			fastAck2 := e2.(*yagpaxosproto.MFastAck)
			return fastAck1.Dep.Equals(fastAck2.Dep)
		}
		r.fastAckQuorumSets[msg.Instance] =
			newQuorumSet(fastQuorumSize, related)
		qs = r.fastAckQuorumSets[msg.Instance]
	}

	qs.add(msg)
	r.Unlock()
	go func() {
		time.Sleep(100 * time.Millisecond)
		qs.stop <- nil
	}()
	es, err := qs.wait(r)
	r.Lock()
	if es == nil && err == nil {
		return
	}

	defer func() {
		delete(r.fastAckQuorumSets, msg.Instance)
	}()

	leaderId := leader(r.ballot, r.N)
	getLeaderMsg := func(es []interface{}) *yagpaxosproto.MFastAck {
		for _, e := range es {
			if (e.(*yagpaxosproto.MFastAck)).Replica == leaderId {
				return e.(*yagpaxosproto.MFastAck)
			}
		}
		return nil
	}

	if err == nil {
		leaderMsg := getLeaderMsg(es)
		if leaderMsg == nil {
			return
		}

		commit := &yagpaxosproto.MCommit{
			Replica:  r.Id,
			Instance: msg.Instance,
			Command:  msg.Command,
			Dep:      r.deps[msg.Instance],
		}
		// TODO: do not send commit to all peers
		// if follower
		r.sendToAll(commit, r.cs.commitRPC)
		go r.handleCommit(commit)
	} else {
		var leaderMsg *yagpaxosproto.MFastAck
		qs.sortBySize()
		for _, q := range qs.quorums {
			if q.size < slowQuorumSize {
				return
			}
			leaderMsg = getLeaderMsg(q.elements)
			if leaderMsg != nil {
				break
			}
		}
		if leaderMsg == nil {
			return
		}

		r.phases[msg.Instance] = SLOW_ACCEPT
		if r.status == FOLLOWER {
			r.cmds[msg.Instance] = leaderMsg.Command
			r.deps[msg.Instance] = leaderMsg.Dep
		}

		slowAck := &yagpaxosproto.MSlowAck{
			Replica:  r.Id,
			Ballot:   r.ballot,
			Instance: msg.Instance,
			Command:  r.cmds[msg.Instance],
			Dep:      r.deps[msg.Instance],
		}
		if r.status == FOLLOWER {
			r.SendMsg(leaderMsg.Replica, r.cs.slowAckRPC, slowAck)
		} else {
			go r.handleSlowAck(slowAck)
		}
	}
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
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER || msg.Ballot != r.ballot {
		return
	}

	qs, exists := r.slowAckQuorumSets[msg.Instance]
	if !exists {
		related := func(e1 interface{}, e2 interface{}) bool {
			slowAck1 := e1.(*yagpaxosproto.MSlowAck)
			slowAck2 := e2.(*yagpaxosproto.MSlowAck)
			return slowAck1.Dep.Equals(slowAck2.Dep)
		}
		r.slowAckQuorumSets[msg.Instance] =
			newQuorumSet(r.N>>1, related)
		qs = r.slowAckQuorumSets[msg.Instance]
	}

	qs.add(msg)
	r.Unlock()
	es, _ := qs.wait(r)
	r.Lock()
	if es == nil {
		return
	}

	defer func() {
		delete(r.slowAckQuorumSets, msg.Instance)
	}()

	if (es[0].(*yagpaxosproto.MSlowAck)).Dep.Equals(r.deps[msg.Instance]) {
		commit := &yagpaxosproto.MCommit{
			Replica:  r.Id,
			Instance: msg.Instance,
			Command:  r.cmds[msg.Instance],
			Dep:      r.deps[msg.Instance],
		}
		r.sendToAll(commit, r.cs.commitRPC)
		go r.handleCommit(commit)
	}
}

func (r *Replica) handleNewLeader(msg *yagpaxosproto.MNewLeader) {
	r.Lock()
	defer r.Unlock()

	if r.ballot >= msg.Ballot {
		return
	}

	r.ballot = msg.Ballot
	if leader(r.ballot, r.N) == r.Id {
		r.status = LEADER
	} else {
		r.status = FOLLOWER
	}
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

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs,
	reply *genericsmrproto.BeTheLeaderReply) error {
	r.Lock()
	defer r.Unlock()

	oldLeader := leader(r.ballot, r.N)
	newBallot := r.ballot - oldLeader + r.Id
	if r.Id <= oldLeader {
		newBallot += int32(r.N)
	}

	newLeader := &yagpaxosproto.MNewLeader{
		Replica: r.Id,
		Ballot:  newBallot,
	}
	r.sendToAll(newLeader, r.cs.newLeaderRPC)
	go r.handleNewLeader(newLeader)

	return nil
}

func (r *Replica) sendToAll(msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		r.M.Lock()
		if r.Alive[p] {
			r.M.Unlock()
			r.SendMsg(p, rpc, msg)
			r.M.Lock()
		}
		r.M.Unlock()
	}
}

func leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func inConflict(c1 state.Command, c2 state.Command) bool {
	return false
}

type quorum struct {
	// not the desired size, but the actual one
	size     int
	elements []interface{}
}

type quorumSet struct {
	neededSize int
	quorums    []*quorum
	related    func(interface{}, interface{}) bool
	out        chan []interface{}
	stop       chan interface{}
	waiting    bool
}

func newQuorumSet(size int,
	relation func(interface{}, interface{}) bool) *quorumSet {
	return &quorumSet{
		neededSize: size,
		quorums:    nil,
		related:    relation,
		out:        make(chan []interface{}),
		stop:       make(chan interface{}),
		waiting:    false,
	}
}

func (qs *quorumSet) add(e interface{}) {
	if qs.neededSize < 1 {
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

	qs.quorums = append(qs.quorums, &quorum{
		size:     1,
		elements: make([]interface{}, qs.neededSize),
	})
	qs.quorums[len(qs.quorums)-1].elements[0] = e

	if qs.neededSize < 2 {
		qs.out <- qs.quorums[len(qs.quorums)-1].elements
	}
}

func (qs *quorumSet) getLargestSet() ([]interface{}, int) {
	size := 0
	var set []interface{}

	for _, q := range qs.quorums {
		if size < q.size {
			size = q.size
			set = q.elements
		}
	}

	return set, size
}

func (qs *quorumSet) sortBySize() {
	sort.Slice(qs.quorums, func(i, j int) bool {
		return qs.quorums[i].size > qs.quorums[j].size
	})
}

func (qs *quorumSet) wait(m interface {
	Lock()
	Unlock()
}) ([]interface{}, error) {
	if m != nil {
		m.Lock()
	}

	if qs.waiting {
		if m != nil {
			m.Unlock()
		}
		return nil, nil
	}

	qs.waiting = true
	defer func() { qs.waiting = false }()

	if m != nil {
		m.Unlock()
	}

	select {
	case q := <-qs.out:
		return q, nil
	case <-qs.stop:
		return nil, errors.New("Stopped")
	}

	return nil, nil
}
