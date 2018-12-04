package yagpaxos

import (
	"bufio"
	"errors"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"sort"
	"state"
	"sync"
	"time"
	"yagpaxosproto"
)

type Replica struct {
	*genericsmr.Replica
	sync.Mutex

	status  int
	ballot  int32
	cballot int32

	phases map[int32]int
	cmds   map[int32]state.Command
	deps   map[int32]yagpaxosproto.DepSet

	slowAckQuorumSets     map[int32]*quorumSet
	fastAckQuorumSets     map[int32]*quorumSet
	newLeaderAckQuorumSet *quorumSet
	syncAckQuorumSet      *quorumSet

	cs CommunicationSupply
}

// status
const (
	LEADER = iota
	FOLLOWER
	PREPARING
)

// phase
const (
	START = iota
	FAST_ACCEPT
	SLOW_ACCEPT
	COMMIT
	DELIVER
)

type CommunicationSupply struct {
	maxLatency time.Duration

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

	proposeReplies map[int32]*bufio.Writer
	proposeLocks   map[int32]*sync.Mutex
	timestamps     map[int32]int64
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty bool, exec bool, lread bool, dreply bool) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply),

		status:  FOLLOWER,
		ballot:  0,
		cballot: 0,

		phases: make(map[int32]int),
		cmds:   make(map[int32]state.Command),
		deps:   make(map[int32]yagpaxosproto.DepSet),

		slowAckQuorumSets: make(map[int32]*quorumSet),
		fastAckQuorumSets: make(map[int32]*quorumSet),

		cs: CommunicationSupply{
			maxLatency: 0,

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

			proposeReplies: make(map[int32]*bufio.Writer),
			proposeLocks:   make(map[int32]*sync.Mutex),
			timestamps:     make(map[int32]int64),
		},
	}

	r.newLeaderAckQuorumSet = newQuorumSet(r.N/2+1,
		func(e1 interface{}, e2 interface{}) bool { return true })
	r.syncAckQuorumSet = newQuorumSet(r.N/2,
		func(e1 interface{}, e2 interface{}) bool { return true })

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
	r.ConnectToPeers()
	latencies := r.ComputeClosestPeers()
	for _, l := range latencies {
		d := time.Duration(l*1000*1000) * time.Nanosecond
		if d > r.cs.maxLatency {
			r.cs.maxLatency = d
		}
	}

	go r.WaitForClientConnections()

	go func() {
		for !r.Shutdown {
			time.Sleep(4 * time.Second)
			r.execute()
		}
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

	// TODO: ignore duplicates

	r.deps[msg.CommandId] = yagpaxosproto.NewDepSet()
	for cid, p := range r.phases {
		if p != START && inConflict(r.cmds[cid], msg.Command) {
			r.deps[msg.CommandId].Add(cid)
		}
	}
	r.phases[msg.CommandId] = FAST_ACCEPT
	r.cmds[msg.CommandId] = msg.Command
	r.cs.proposeReplies[msg.CommandId] = msg.Reply
	r.cs.proposeLocks[msg.CommandId] = msg.Mutex
	r.cs.timestamps[msg.CommandId] = msg.Timestamp

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
		r.ballot != msg.Ballot || r.phases[msg.Instance] == COMMIT ||
		r.phases[msg.Instance] == DELIVER {
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

	qs.add(msg, msg.Replica == leader(r.ballot, r.N))
	qs.wakeupAfter(10 * r.cs.maxLatency) // FIXME
	r.Unlock()
	q, err := qs.wait(r)
	r.Lock()
	if q == nil && err == nil ||
		r.phases[msg.Instance] == COMMIT || r.phases[msg.Instance] == DELIVER {
		return
	}

	defer qs.reset()

	getLeaderMsg := func(q *quorum) *yagpaxosproto.MFastAck {
		leaderMsg := q.getLeaderMsg()
		if leaderMsg != nil {
			return leaderMsg.(*yagpaxosproto.MFastAck)
		}

		if r.status == LEADER {
			someMsg := q.elements[0].(*yagpaxosproto.MFastAck)
			if someMsg.Dep.Equals(r.deps[msg.Instance]) {
				return &yagpaxosproto.MFastAck{
					Replica:  r.Id,
					Ballot:   r.ballot,
					Instance: msg.Instance,
					Command:  r.cmds[msg.Instance],
					Dep:      r.deps[msg.Instance],
				}
			}
		}

		return nil
	}

	var leaderMsg *yagpaxosproto.MFastAck
	if q != nil {
		leaderMsg = getLeaderMsg(q)
	}

	if err == nil && leaderMsg != nil {
		commit := &yagpaxosproto.MCommit{
			Replica:  r.Id,
			Instance: msg.Instance,
			Command:  msg.Command,
			Dep:      r.deps[msg.Instance],
		}

		if r.status == FOLLOWER {
			r.SendMsg(leaderMsg.Replica, r.cs.commitRPC, commit)
		} else {
			r.sendToAll(commit, r.cs.commitRPC)
		}
		go r.handleCommit(commit)
	} else {
		leaderMsg = nil
		qs.sortBySize()
		for _, q := range qs.quorums {
			if q.size < slowQuorumSize {
				return
			}
			leaderMsg = getLeaderMsg(q)
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
	defer func() {
		r.Unlock()
		r.execute()
	}()

	if (r.status != LEADER && r.status != FOLLOWER) ||
		r.phases[msg.Instance] == DELIVER {
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
			newQuorumSet(r.N/2+1, related)
		qs = r.slowAckQuorumSets[msg.Instance]
	}

	qs.add(msg, msg.Replica == leader(r.ballot, r.N))
	qs.wakeupAfter(10 * r.cs.maxLatency) // FIXME
	r.Unlock()
	q, err := qs.wait(r)
	r.Lock()
	if q == nil {
		if err != nil {
			qs.reset()
		}
		return
	}

	defer qs.reset()

	someMsg := q.elements[0].(*yagpaxosproto.MSlowAck)
	if someMsg.Dep.Equals(r.deps[msg.Instance]) {
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

	r.status = PREPARING
	r.ballot = msg.Ballot

	newLeaderAck := &yagpaxosproto.MNewLeaderAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cballot: r.cballot,
		Phases:  r.phases,
		Cmds:    r.cmds,
		Deps:    r.deps,
	}

	if l := leader(r.ballot, r.N); l == r.Id {
		go r.handleNewLeaderAck(newLeaderAck)
	} else {
		r.SendMsg(l, r.cs.newLeaderAckRPC, newLeaderAck)
	}
}

func (r *Replica) handleNewLeaderAck(msg *yagpaxosproto.MNewLeaderAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || r.ballot != msg.Ballot {
		return
	}

	r.newLeaderAckQuorumSet.add(msg, false)
	r.newLeaderAckQuorumSet.wakeupAfter(10 * time.Second) // FIXME
	r.Unlock()
	q, err := r.newLeaderAckQuorumSet.wait(r)
	r.Lock()
	if q == nil {
		if err != nil {
			r.newLeaderAckQuorumSet.reset()
		}
		return
	}

	defer r.newLeaderAckQuorumSet.reset()

	maxCballot := int32(0)
	for _, e := range q.elements {
		newLeaderAck := e.(*yagpaxosproto.MNewLeaderAck)
		if newLeaderAck.Cballot > maxCballot {
			maxCballot = newLeaderAck.Cballot
		}
	}

	moreThanFourth := func(cmdId int32) *yagpaxosproto.MNewLeaderAck {
		n := 1
		for _, e0 := range q.elements {
			newLeaderAck0 := e0.(*yagpaxosproto.MNewLeaderAck)
			d := newLeaderAck0.Deps[cmdId]
			for _, e := range q.elements {
				newLeaderAck := e.(*yagpaxosproto.MNewLeaderAck)
				if d.Equals(newLeaderAck.Deps[cmdId]) {
					n++
				}
				if n >= r.N/4 {
					return newLeaderAck0
				}
			}
		}
		return nil
	}

	cmdIds := make(map[int32]struct{})

	for _, e := range q.elements {
		newLeaderAck := e.(*yagpaxosproto.MNewLeaderAck)
		for cmdId, p := range newLeaderAck.Phases {
			_, exists := cmdIds[cmdId]
			if !exists {
				cmdIds[cmdId] = struct{}{}
				r.deps[cmdId] = nil
			}
			if r.deps[cmdId] == nil &&
				(p == COMMIT ||
					(p == SLOW_ACCEPT && newLeaderAck.Cballot == maxCballot)) {
				r.phases[cmdId] = newLeaderAck.Phases[cmdId]
				r.cmds[cmdId] = newLeaderAck.Cmds[cmdId]
				r.deps[cmdId] = newLeaderAck.Deps[cmdId]
			} else if r.deps[cmdId] == nil {
				someMsg := moreThanFourth(cmdId)
				if someMsg != nil {
					r.phases[cmdId] = SLOW_ACCEPT
					r.cmds[cmdId] = someMsg.Cmds[cmdId]
					r.deps[cmdId] = someMsg.Deps[cmdId]
				}
			}
		}
	}

	sync := &yagpaxosproto.MSync{
		Replica: r.Id,
		Ballot:  r.ballot,
		Phases:  r.phases,
		Cmds:    r.cmds,
		Deps:    r.deps,
	}
	r.sendToAll(sync, r.cs.syncRPC)
}

func (r *Replica) handleSync(msg *yagpaxosproto.MSync) {
	r.Lock()
	defer r.Unlock()

	if r.ballot > msg.Ballot {
		return
	}

	r.status = FOLLOWER
	r.ballot = msg.Ballot
	r.cballot = msg.Ballot
	r.phases = msg.Phases
	r.cmds = msg.Cmds
	r.deps = msg.Deps

	syncAck := &yagpaxosproto.MSyncAck{
		Replica: r.Id,
		Ballot:  msg.Ballot,
	}
	r.SendMsg(leader(msg.Ballot, r.N), r.cs.syncAckRPC, syncAck)
}

func (r *Replica) handleSyncAck(msg *yagpaxosproto.MSyncAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || r.ballot != msg.Ballot {
		return
	}

	r.syncAckQuorumSet.add(struct{}{}, false)
	r.syncAckQuorumSet.wakeupAfter(10 * r.cs.maxLatency) // FIXME
	r.Unlock()
	q, err := r.syncAckQuorumSet.wait(r)
	r.Lock()
	if q == nil {
		if err != nil {
			r.syncAckQuorumSet.reset()
		}
		return
	}

	defer r.syncAckQuorumSet.reset()

	r.status = LEADER
	for cmdId, p := range r.phases {
		// TODO: send commit even if p == DELIVER
		if p == COMMIT || p == DELIVER {
			return
		}

		commit := &yagpaxosproto.MCommit{
			Replica:  r.Id,
			Instance: cmdId,
			Command:  r.cmds[cmdId],
			Dep:      r.deps[cmdId],
		}
		r.sendToAll(commit, r.cs.commitRPC)
		go r.handleCommit(commit)
	}
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

func inConflict(c1, c2 state.Command) bool {
	return state.Conflict(&c1, &c2)
}

func (r *Replica) execute() {
	r.Lock()
	defer r.Unlock()

	for cmdId, p := range r.phases {
		if p != COMMIT {
			continue
		}

		exec := true
		for depId := range r.deps[cmdId] {
			if r.phases[depId] != DELIVER {
				exec = false
				break
			}
		}
		if !exec {
			continue
		}

		r.phases[cmdId] = DELIVER

		if !r.Exec {
			continue
		}
		cmd := r.cmds[cmdId]
		v := cmd.Execute(r.State)

		if r.status != LEADER || !r.Dreply {
			continue
		}

		proposeReply := &genericsmrproto.ProposeReplyTS{
			OK:        genericsmr.TRUE,
			CommandId: cmdId,
			Value:     v,
			Timestamp: r.cs.timestamps[cmdId],
		}
		r.ReplyProposeTS(proposeReply,
			r.cs.proposeReplies[cmdId],
			r.cs.proposeLocks[cmdId])
	}
}

type quorum struct {
	// not the desired size, but the actual one
	size     int
	leaderId int
	elements []interface{}
}

type quorumSet struct {
	*sync.Once
	neededSize int
	quorums    []*quorum
	related    func(interface{}, interface{}) bool
	out        chan *quorum
	stop       chan interface{}
	waiting    bool
}

func newQuorumSet(size int,
	relation func(interface{}, interface{}) bool) *quorumSet {
	return &quorumSet{
		Once:       &sync.Once{},
		neededSize: size,
		quorums:    nil,
		related:    relation,
		out:        make(chan *quorum, size),
		stop:       make(chan interface{}, 1),
		waiting:    false,
	}
}

func (q *quorum) getLeaderMsg() interface{} {
	if q.leaderId != -1 {
		return q.elements[q.leaderId]
	}
	return nil
}

func (qs *quorumSet) add(e interface{}, fromLeader bool) {
	if qs.neededSize < 1 {
		return
	}

	for _, q := range qs.quorums {
		if qs.related(q.elements[0], e) {
			if q.size >= qs.neededSize {
				if fromLeader && q.leaderId == -1 {
					q.elements = append(q.elements, e)
					q.leaderId = q.size
					q.size++
				}
				qs.out <- q
				return
			}
			q.elements[q.size] = e
			if fromLeader {
				q.leaderId = q.size
			}
			q.size++
			if q.size >= qs.neededSize {
				qs.out <- q
			}
			return
		}
	}

	i := len(qs.quorums)
	qs.quorums = append(qs.quorums, &quorum{
		size:     1,
		leaderId: -1,
		elements: make([]interface{}, qs.neededSize),
	})
	qs.quorums[i].elements[0] = e
	if fromLeader {
		qs.quorums[i].leaderId = 0
	}

	if qs.neededSize < 2 {
		qs.out <- qs.quorums[i]
	}
}

func (qs *quorumSet) sortBySize() {
	sort.Slice(qs.quorums, func(i, j int) bool {
		return qs.quorums[i].size > qs.quorums[j].size
	})
}

func (qs *quorumSet) wakeupAfter(d time.Duration) {
	qs.Do(func() {
		go func() {
			time.Sleep(d)
			qs.stop <- nil
		}()
	})
}

func (qs *quorumSet) reset() {
	qs.waiting = false
}

func (qs *quorumSet) wait(m interface {
	Lock()
	Unlock()
}) (*quorum, error) {
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
	if m != nil {
		m.Unlock()
	}

	select {
	case q := <-qs.out:
		if m != nil {
			m.Lock()
		}
		qs.Once = &sync.Once{}
		if m != nil {
			m.Unlock()
		}
		return q, nil
	case <-qs.stop:
		return nil, errors.New("Stopped")
	}

	return nil, nil
}
