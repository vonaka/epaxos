package yagpaxos

import (
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
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

	committer *committer
	gc        *gc
	proposes  map[int32]*genericsmr.Propose

	ignoreCommitted bool

	fastAckQuorumSets      map[int32]*quorumSet
	slowAckQuorumSets      map[int32]*quorumSet
	newLeaderAckQuorumSets map[int32]*quorumSet
	syncAckQuorumSets      map[int32]*quorumSet

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
	collectChan      chan fastrpc.Serializable

	fastAckRPC      uint8
	commitRPC       uint8
	slowAckRPC      uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	syncRPC         uint8
	syncAckRPC      uint8
	collectRPC      uint8
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty bool, exec bool, lread bool,
	dreply bool, ignoreCommitted bool) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply),

		status:  FOLLOWER,
		ballot:  0,
		cballot: 0,

		phases: make(map[int32]int),
		cmds:   make(map[int32]state.Command),
		deps:   make(map[int32]yagpaxosproto.DepSet),

		proposes: make(map[int32]*genericsmr.Propose),

		ignoreCommitted: ignoreCommitted,

		fastAckQuorumSets:      make(map[int32]*quorumSet),
		slowAckQuorumSets:      make(map[int32]*quorumSet),
		newLeaderAckQuorumSets: make(map[int32]*quorumSet),
		syncAckQuorumSets:      make(map[int32]*quorumSet),

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
			collectChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
		},
	}
	r.committer = newCommitter(&(r.Mutex), &(r.Shutdown))
	r.gc = newGc(func(cmdId int32) {
		_, exists := r.phases[cmdId]
		if exists {
			delete(r.phases, cmdId)
		}
		_, exists = r.cmds[cmdId]
		if exists {
			delete(r.cmds, cmdId)
		}
		_, exists = r.deps[cmdId]
		if exists {
			delete(r.deps, cmdId)
		}
		_, exists = r.proposes[cmdId]
		if exists {
			delete(r.proposes, cmdId)
		}
		_, exists = r.fastAckQuorumSets[cmdId]
		if exists {
			delete(r.fastAckQuorumSets, cmdId)
		}
		_, exists = r.slowAckQuorumSets[cmdId]
		if exists {
			delete(r.slowAckQuorumSets, cmdId)
		}
		_, exists = r.newLeaderAckQuorumSets[cmdId]
		if exists {
			delete(r.newLeaderAckQuorumSets, cmdId)
		}
		_, exists = r.syncAckQuorumSets[cmdId]
		if exists {
			delete(r.syncAckQuorumSets, cmdId)
		}
	}, &r.Mutex, &r.Shutdown)

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
	r.cs.collectRPC =
		r.RegisterRPC(new(yagpaxosproto.MCollect), r.cs.collectChan)

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
		case m := <-r.cs.collectChan:
			collect := m.(*yagpaxosproto.MCollect)
			go r.handleCollect(collect)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER && r.status != FOLLOWER {
		return
	}

	_, exists := r.proposes[msg.CommandId]
	if exists {
		return
	}
	r.proposes[msg.CommandId] = msg

	_, exists = r.cmds[msg.CommandId]
	if !exists {
		r.phases[msg.CommandId] = FAST_ACCEPT
		r.cmds[msg.CommandId] = msg.Command
		r.deps[msg.CommandId] = yagpaxosproto.NewDepSet()

		if r.ignoreCommitted {
			r.committer.undeliveredIter(func(cid int32) {
				if cid != msg.CommandId &&
					r.phases[cid] != START &&
					r.phases[cid] != COMMIT &&
					inConflict(r.cmds[cid], msg.Command) {
					r.deps[msg.CommandId].Add(cid)
				}
			})
		} else {
			for cid, p := range r.phases {
				if cid != msg.CommandId && p != START &&
					inConflict(r.cmds[cid], msg.Command) {
					r.deps[msg.CommandId].Add(cid)
				}
			}
		}
	}

	fastAck := &yagpaxosproto.MFastAck{
		Replica:   r.Id,
		Ballot:    r.ballot,
		CommandId: msg.CommandId,
		Command:   msg.Command,
		Dep:       r.deps[msg.CommandId],
	}
	if r.status == LEADER {
		r.committer.add(msg.CommandId)
		fastAck.AcceptId = r.committer.getInstance(msg.CommandId)
	} else {
		fastAck.AcceptId = -1
	}
	go r.sendToAll(fastAck, r.cs.fastAckRPC)
	go r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *yagpaxosproto.MFastAck) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.fastAckQuorumSets[msg.CommandId]

	if !exists {
		fastQuorumSize := 3*r.N/4 + 1
		waitFor := time.Duration(r.N+1) * r.cs.maxLatency // FIXME
		related := func(e1 interface{}, e2 interface{}) bool {
			fastAck1 := e1.(*yagpaxosproto.MFastAck)
			fastAck2 := e2.(*yagpaxosproto.MFastAck)
			return fastAck1.Dep.SmartEquals(fastAck2.Dep,
				func(cmdId int32, _ bool) bool {
					p, exists := r.phases[cmdId]
					return exists && (p == COMMIT || p == DELIVER)
				})
		}
		wakeup := func() bool {
			_, exists := r.proposes[msg.CommandId]
			return exists
		}
		qs = newQuorumSet(fastQuorumSize, waitFor, related,
			wakeup, r.handleFastAcks, true)
		r.fastAckQuorumSets[msg.CommandId] = qs
	}

	fromLeader := msg.Replica == leader(r.ballot, r.N)
	if fromLeader && r.status == FOLLOWER {
		r.committer.addTo(msg.CommandId, msg.AcceptId)
	}
	qs.add(msg, fromLeader)
}

func (r *Replica) handleFastAcks(q *quorum) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || q == nil {
		return
	}

	fastQuorumSize := 3*r.N/4 + 1
	slowQuorumSize := r.N/2 + 1

	leaderMsg := q.getLeaderMsg()
	if leaderMsg == nil {
		return
	}
	leaderFastAck := leaderMsg.(*yagpaxosproto.MFastAck)
	if r.ballot != leaderFastAck.Ballot {
		return
	}

	_, exists := r.fastAckQuorumSets[leaderFastAck.CommandId]
	if !exists ||
		r.fastAckQuorumSets[leaderFastAck.CommandId].totalSize <
			slowQuorumSize {
		return
	}

	if q.size >= fastQuorumSize {
		commit := &yagpaxosproto.MCommit{
			Replica:   r.Id,
			CommandId: leaderFastAck.CommandId,
			Command:   leaderFastAck.Command,
			Dep:       leaderFastAck.Dep,
		}

		if r.status == FOLLOWER {
			go r.SendMsg(leaderFastAck.Replica, r.cs.commitRPC, commit)
		} else {
			go r.sendToAll(commit, r.cs.commitRPC)
		}
		go r.handleCommit(commit)
	} else {
		r.phases[leaderFastAck.CommandId] = SLOW_ACCEPT
		if r.status == FOLLOWER {
			r.cmds[leaderFastAck.CommandId] = leaderFastAck.Command
			r.deps[leaderFastAck.CommandId] = leaderFastAck.Dep
		}

		slowAck := &yagpaxosproto.MSlowAck{
			Replica:   r.Id,
			Ballot:    leaderFastAck.Ballot,
			CommandId: leaderFastAck.CommandId,
		}
		if r.status == FOLLOWER {
			go r.SendMsg(leaderFastAck.Replica, r.cs.slowAckRPC, slowAck)
		} else {
			go r.handleSlowAck(slowAck)
		}
	}
}

func (r *Replica) handleCollect(msg *yagpaxosproto.MCollect) {
	r.Lock()
	defer r.Unlock()
	r.gc.collect(msg.CommandId, msg.Replica, r.N)
}

func (r *Replica) handleCommit(msg *yagpaxosproto.MCommit) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER && r.status != FOLLOWER {
		return
	} else if r.phases[msg.CommandId] == DELIVER {
		go r.handleCollect(&yagpaxosproto.MCollect{
			Replica:   msg.Replica,
			CommandId: msg.CommandId,
		})
		return
	}

	r.phases[msg.CommandId] = COMMIT
	r.cmds[msg.CommandId] = msg.Command
	r.deps[msg.CommandId] = msg.Dep

	f := func(cmdId int32) {
		if r.phases[cmdId] != COMMIT {
			return
		}
		r.phases[cmdId] = DELIVER

		if !r.Exec {
			return
		}
		cmd := r.cmds[cmdId]
		v := cmd.Execute(r.State)

		p, exists := r.proposes[cmdId]
		if !exists || !r.Dreply {
			return
		}

		proposeReply := &genericsmrproto.ProposeReplyTS{
			OK:        genericsmr.TRUE,
			CommandId: cmdId,
			Value:     v,
			Timestamp: p.Timestamp,
		}
		r.ReplyProposeTS(proposeReply, p.Reply, p.Mutex)
	}

	if r.status == LEADER {
		r.committer.deliver(msg.CommandId, f)
	} else if r.status == FOLLOWER {
		r.committer.safeDeliver(msg.CommandId, msg.Dep, f)
	}

	collect := &yagpaxosproto.MCollect{
		Replica:   r.Id,
		CommandId: msg.CommandId,
	}
	go r.sendToAll(collect, r.cs.collectRPC)
	go r.handleCollect(collect)
	go r.handleCollect(&yagpaxosproto.MCollect{
		Replica:   msg.Replica,
		CommandId: msg.CommandId,
	})
}

func (r *Replica) handleSlowAck(msg *yagpaxosproto.MSlowAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.slowAckQuorumSets[msg.CommandId]
	if !exists {
		slowQuorumSize := r.N/2 + 1
		waitFor := time.Duration(r.N+1) * r.cs.maxLatency // FIXME
		related := func(e1 interface{}, e2 interface{}) bool {
			return true
		}
		wakeup := func() bool {
			return true
		}
		qs = newQuorumSet(slowQuorumSize, waitFor, related,
			wakeup, r.handleSlowAcks, true)
		r.slowAckQuorumSets[msg.CommandId] = qs
	}

	qs.add(msg, msg.Replica == leader(r.ballot, r.N))
}

func (r *Replica) handleSlowAcks(q *quorum) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER || q == nil {
		return
	}

	slowQuorumSize := r.N/2 + 1

	if q.size < slowQuorumSize {
		return
	}

	leaderMsg := q.getLeaderMsg()
	if leaderMsg == nil {
		return
	}
	leaderSlowAck := leaderMsg.(*yagpaxosproto.MSlowAck)
	if r.ballot != leaderSlowAck.Ballot {
		return
	}

	commit := &yagpaxosproto.MCommit{
		Replica:   r.Id,
		CommandId: leaderSlowAck.CommandId,
		Command:   r.cmds[leaderSlowAck.CommandId],
		Dep:       r.deps[leaderSlowAck.CommandId],
	}
	go r.sendToAll(commit, r.cs.commitRPC)
	go r.handleCommit(commit)
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
		go r.SendMsg(l, r.cs.newLeaderAckRPC, newLeaderAck)
	}
}

func (r *Replica) handleNewLeaderAck(msg *yagpaxosproto.MNewLeaderAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.newLeaderAckQuorumSets[msg.Ballot]
	if !exists {
		slowQuorumSize := r.N/2 + 1
		waitFor := 60 * time.Minute // FIXME
		related := func(e1 interface{}, e2 interface{}) bool {
			return true
		}
		wakeup := func() bool {
			return true
		}
		qs = newQuorumSet(slowQuorumSize, waitFor, related,
			wakeup, r.handleNewLeaderAcks, false)
		r.newLeaderAckQuorumSets[msg.Ballot] = qs
	}

	qs.add(msg, msg.Replica == leader(r.ballot, r.N))
}

func (r *Replica) handleNewLeaderAcks(q *quorum) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || q == nil {
		return
	}

	slowQuorumSize := r.N/2 + 1

	if q.size < slowQuorumSize {
		return
	}

	someMsg := q.elements[0].(*yagpaxosproto.MNewLeaderAck)
	if r.ballot != someMsg.Ballot {
		return
	}

	// TODO: reset local state

	maxCballot := int32(0)
	for _, e := range q.elements {
		newLeaderAck := e.(*yagpaxosproto.MNewLeaderAck)
		if newLeaderAck.Cballot > maxCballot {
			maxCballot = newLeaderAck.Cballot
		}
	}

	moreThanFourth := func(cmdId int32) *yagpaxosproto.MNewLeaderAck {
		n := 1
		for id0, e0 := range q.elements {
			newLeaderAck0 := e0.(*yagpaxosproto.MNewLeaderAck)
			d := newLeaderAck0.Deps[cmdId]
			for id, e := range q.elements {
				if id == id0 {
					continue
				}

				newLeaderAck := e.(*yagpaxosproto.MNewLeaderAck)
				d2 := newLeaderAck.Deps[cmdId]

				if d.SmartEquals(d2, func(cmdId int32, dContains bool) bool {
					if dContains {
						p, exists := newLeaderAck0.Phases[cmdId]
						return exists && (p == COMMIT || p == DELIVER)
					} else {
						p, exists := newLeaderAck.Phases[cmdId]
						return exists && (p == COMMIT || p == DELIVER)
					}
				}) {
					n++
				}

				if n >= r.N/4 { // maybe n > r.N.4 ?
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
				r.deps[cmdId] = yagpaxosproto.NilDepSet()
			}
			if r.deps[cmdId].IsNil() &&
				(p == COMMIT ||
					(p == SLOW_ACCEPT && newLeaderAck.Cballot == maxCballot)) {
				r.phases[cmdId] = newLeaderAck.Phases[cmdId]
				r.cmds[cmdId] = newLeaderAck.Cmds[cmdId]
				r.deps[cmdId] = newLeaderAck.Deps[cmdId]
			} else if r.deps[cmdId].IsNil() {
				someMsg := moreThanFourth(cmdId)
				if someMsg != nil {
					r.phases[cmdId] = SLOW_ACCEPT
					r.cmds[cmdId] = someMsg.Cmds[cmdId]
					r.deps[cmdId] = someMsg.Deps[cmdId]
				}
			}
		}
	}

	for _, dep := range r.deps {
		dep.Iter(func(cmdId int32) bool {
			p, exists := r.phases[cmdId]
			if !exists || p == START {
				r.phases[cmdId] = SLOW_ACCEPT
				r.cmds[cmdId] = state.NOOP()[0]
				// TODO: set r.deps[cmdId] to NIL
			}
			return false
		})
	}

	r.cballot = r.ballot

	sync := &yagpaxosproto.MSync{
		Replica: r.Id,
		Ballot:  r.ballot,
		Phases:  r.phases,
		Cmds:    r.cmds,
		Deps:    r.deps,
	}
	go r.sendToAll(sync, r.cs.syncRPC)
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
	go r.SendMsg(leader(msg.Ballot, r.N), r.cs.syncAckRPC, syncAck)
}

func (r *Replica) handleSyncAck(msg *yagpaxosproto.MSyncAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.syncAckQuorumSets[msg.Ballot]
	if !exists {
		slowQuorumSize := r.N / 2
		waitFor := 60 * time.Minute // FIXME
		related := func(e1 interface{}, e2 interface{}) bool {
			return true
		}
		wakeup := func() bool {
			return true
		}
		qs = newQuorumSet(slowQuorumSize, waitFor, related,
			wakeup, r.handleSyncAcks, false)
		r.syncAckQuorumSets[msg.Ballot] = qs
	}

	qs.add(msg, msg.Replica == leader(r.ballot, r.N))
}

func (r *Replica) handleSyncAcks(q *quorum) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || q == nil {
		return
	}

	slowQuorumSize := r.N / 2

	if q.size < slowQuorumSize {
		return
	}

	someMsg := q.elements[0].(*yagpaxosproto.MSyncAck)
	if r.ballot != someMsg.Ballot {
		return
	}

	r.status = LEADER
	for cmdId, p := range r.phases {
		// TODO: send commit even if p == DELIVER
		if p == COMMIT || p == DELIVER {
			return
		}

		commit := &yagpaxosproto.MCommit{
			Replica:   r.Id,
			CommandId: cmdId,
			Command:   r.cmds[cmdId],
			Dep:       r.deps[cmdId],
		}
		go r.sendToAll(commit, r.cs.commitRPC)
		go r.handleCommit(commit)
	}
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs,
	reply *genericsmrproto.BeTheLeaderReply) error {
	r.Lock()
	defer r.Unlock()

	if r.N == 1 {
		r.ballot++
		r.status = LEADER
		return nil
	}

	oldLeader := leader(r.ballot, r.N)
	newBallot := r.ballot - oldLeader + r.Id
	if r.Id <= oldLeader {
		newBallot += int32(r.N)
	}

	newLeader := &yagpaxosproto.MNewLeader{
		Replica: r.Id,
		Ballot:  newBallot,
	}
	go r.sendToAll(newLeader, r.cs.newLeaderRPC)
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
