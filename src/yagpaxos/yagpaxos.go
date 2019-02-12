package yagpaxos

import (
	"dlog"
	"errors"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"state"
	"sync"
	"time"
)

type Replica struct {
	*genericsmr.Replica
	sync.Mutex

	status  int
	ballot  int32
	cballot int32

	phases map[CommandId]int
	cmds   map[CommandId]state.Command
	deps   map[CommandId]DepVector

	vectors map[state.Key]*DepVector

	committer *committer
	gc        *gc
	proposes  map[CommandId]*genericsmr.Propose

	ignoreCommitted bool

	fastAckQuorumSets      map[CommandId]*quorumSet
	slowAckQuorumSets      map[CommandId]*quorumSet
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
	thrifty, exec, lread, dreply, ignoreCommitted bool) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply),

		status:  FOLLOWER,
		ballot:  0,
		cballot: 0,

		phases: make(map[CommandId]int),
		cmds:   make(map[CommandId]state.Command),
		deps:   make(map[CommandId]DepVector),

		vectors: make(map[state.Key]*DepVector),

		proposes: make(map[CommandId]*genericsmr.Propose),

		ignoreCommitted: ignoreCommitted,

		fastAckQuorumSets:      make(map[CommandId]*quorumSet),
		slowAckQuorumSets:      make(map[CommandId]*quorumSet),
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
	r.gc = newGc(func(cmdId CommandId) {
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

		/* Wrong! Keys are ballots!

		_, exists = r.newLeaderAckQuorumSets[cmdId]
		if exists {
			delete(r.newLeaderAckQuorumSets, cmdId)
		}
		_, exists = r.syncAckQuorumSets[cmdId]
		if exists {
			delete(r.syncAckQuorumSets, cmdId)
		} */

	}, &r.Mutex, &r.Shutdown)

	r.cs.fastAckRPC =
		r.RegisterRPC(new(MFastAck), r.cs.fastAckChan)
	r.cs.commitRPC =
		r.RegisterRPC(new(MCommit), r.cs.commitChan)
	r.cs.slowAckRPC =
		r.RegisterRPC(new(MSlowAck), r.cs.slowAckChan)
	r.cs.newLeaderRPC =
		r.RegisterRPC(new(MNewLeader), r.cs.newLeaderChan)
	r.cs.newLeaderAckRPC =
		r.RegisterRPC(new(MNewLeaderAck), r.cs.newLeaderAckChan)
	r.cs.syncRPC =
		r.RegisterRPC(new(MSync), r.cs.syncChan)
	r.cs.syncAckRPC =
		r.RegisterRPC(new(MSyncAck), r.cs.syncAckChan)
	r.cs.collectRPC =
		r.RegisterRPC(new(MCollect), r.cs.collectChan)

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
			time.Sleep(2 * time.Second)
			r.Lock()
			r.flush()
			r.Unlock()
		}
	}()

	for !r.Shutdown {
		select {
		case propose := <-r.ProposeChan:
			go r.handlePropose(propose)
		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			go r.handleFastAck(fastAck)
		case m := <-r.cs.commitChan:
			commit := m.(*MCommit)
			go r.handleCommit(commit)
		case m := <-r.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			go r.handleSlowAck(slowAck)
		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*MNewLeader)
			go r.handleNewLeader(newLeader)
		case m := <-r.cs.newLeaderAckChan:
			newLeaderAck := m.(*MNewLeaderAck)
			go r.handleNewLeaderAck(newLeaderAck)
		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			go r.handleSync(sync)
		case m := <-r.cs.syncAckChan:
			syncAck := m.(*MSyncAck)
			go r.handleSyncAck(syncAck)
		case m := <-r.cs.collectChan:
			collect := m.(*MCollect)
			go r.handleCollect(collect)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	r.Lock()
	defer r.Unlock()

	if r.status != LEADER && r.status != FOLLOWER {
		go func() {
			time.Sleep(2 * time.Second)
			r.handlePropose(msg)
		}()
		return
	}

	cmdId := CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}

	_, exists := r.proposes[cmdId]
	if exists {
		return
	}
	r.proposes[cmdId] = msg

	_, exists = r.cmds[cmdId]
	if !exists {
		r.phases[cmdId] = FAST_ACCEPT
		r.cmds[cmdId] = msg.Command
		dep, exists := r.vectors[msg.Command.K]
		if exists {
			r.deps[cmdId] = *dep
		} else {
			r.deps[cmdId] = *EmptyVector()
		}

		r.add(&msg.Command, cmdId)
	}

	fastAck := &MFastAck{
		Replica:   r.Id,
		Ballot:    r.ballot,
		CommandId: cmdId,
		Command:   msg.Command,
		Dep:       r.deps[cmdId],
	}
	if r.status == LEADER {
		r.committer.add(cmdId)
		fastAck.AcceptId = r.committer.getInstance(cmdId)
	} else {
		fastAck.AcceptId = -1
	}
	go r.sendToAll(fastAck, r.cs.fastAckRPC)
	go r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *MFastAck) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.fastAckQuorumSets[msg.CommandId]

	if !exists {
		fastQuorumSize := 3*r.N/4 + 1
		slowQuorumSize := r.N/2 + 1

		related := func(e1, e2 interface{}) bool {
			fastAck1 := e1.(*MFastAck)
			fastAck2 := e2.(*MFastAck)
			return fastAck1.Dep.Equals(fastAck2.Dep)
		}

		strongTest := func(q *quorum) bool {
			return q.size >= fastQuorumSize
		}

		weakTest := func(q *quorum) bool {
			return q.size >= slowQuorumSize
		}

		waitFor := time.Duration(r.N+1) * r.cs.maxLatency // FIXME

		qs = newQuorumSet(related, strongTest,
			weakTest, r.handleFastAcks, waitFor)
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

	leaderMsg := q.getLeaderElement()
	if leaderMsg == nil {
		return
	}
	leaderFastAck := leaderMsg.(*MFastAck)
	if r.ballot != leaderFastAck.Ballot {
		return
	}

	if q.size >= fastQuorumSize {
		commit := &MCommit{
			Replica:   r.Id,
			Ballot:    r.ballot,
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

		slowAck := &MSlowAck{
			Replica:   r.Id,
			Ballot:    leaderFastAck.Ballot,
			CommandId: leaderFastAck.CommandId,
			Command:   leaderFastAck.Command,
			Dep:       leaderFastAck.Dep,
		}
		go r.sendToAll(slowAck, r.cs.slowAckRPC)
		go r.handleSlowAck(slowAck)
	}
}

func (r *Replica) handleCollect(msg *MCollect) {
	r.Lock()
	defer r.Unlock()
	r.gc.collect(msg.CommandId, msg.Replica, r.N)
}

func (r *Replica) handleCommit(msg *MCommit) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || msg.Ballot > r.ballot ||
		r.phases[msg.CommandId] == DELIVER {
		return
	}

	r.phases[msg.CommandId] = COMMIT
	r.cmds[msg.CommandId] = msg.Command
	r.deps[msg.CommandId] = msg.Dep

	if r.status == LEADER {
		r.committer.deliver(msg.CommandId, r.executeAndReply)
	} else if r.status == FOLLOWER {
		r.committer.safeDeliver(msg.CommandId, r.executeAndReply)
	}

	if r.committer.wasDelivered(msg.CommandId) {
		collect := &MCollect{
			Replica:   r.Id,
			CommandId: msg.CommandId,
		}
		go r.sendToAll(collect, r.cs.collectRPC)
		go r.handleCollect(collect)
	}
}

func (r *Replica) handleSlowAck(msg *MSlowAck) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.slowAckQuorumSets[msg.CommandId]
	if !exists {
		slowQuorumSize := r.N/2 + 1

		related := func(e1, e2 interface{}) bool {
			return true
		}

		strongTest := func(q *quorum) bool {
			return q.size >= slowQuorumSize
		}

		weakTest := strongTest

		waitFor := time.Duration(0)

		qs = newQuorumSet(related, strongTest,
			weakTest, r.handleSlowAcks, waitFor)
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

	someSlowAck := q.elements[0].(*MSlowAck)
	if r.ballot != someSlowAck.Ballot {
		return
	}

	commit := &MCommit{
		Replica:   r.Id,
		Ballot:    r.ballot,
		CommandId: someSlowAck.CommandId,
		Command:   someSlowAck.Command,
		Dep:       someSlowAck.Dep,
	}
	go r.sendToAll(commit, r.cs.commitRPC)
	go r.handleCommit(commit)
}

func (r *Replica) handleNewLeader(msg *MNewLeader) {
	r.Lock()
	defer r.Unlock()

	if r.ballot >= msg.Ballot {
		return
	}

	r.flush()

	r.status = PREPARING
	r.ballot = msg.Ballot

	newLeaderAck := &MNewLeaderAck{
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
		go func() {
			r.Lock()
			defer r.Unlock()
			r.SendMsg(l, r.cs.newLeaderAckRPC, newLeaderAck)
		}()
	}
}

func (r *Replica) handleNewLeaderAck(msg *MNewLeaderAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.newLeaderAckQuorumSets[msg.Ballot]
	if !exists {
		slowQuorumSize := r.N/2 + 1

		related := func(e1 interface{}, e2 interface{}) bool {
			return true
		}

		// handleNewLeaderAcks must be called only once per ballot
		called := false
		strongTest := func(q *quorum) bool {
			if q.size >= slowQuorumSize {
				defer func() {
					called = true
				}()
				return !called
			}
			return false
		}

		weakTest := strongTest

		waitFor := time.Duration(0)

		qs = newQuorumSet(related, strongTest,
			weakTest, r.handleNewLeaderAcks, waitFor)
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

	someMsg := q.elements[0].(*MNewLeaderAck)
	if r.ballot != someMsg.Ballot {
		return
	}

	maxCballot := int32(0)
	for i := 0; i < q.size; i++ {
		e := q.elements[i]
		newLeaderAck := e.(*MNewLeaderAck)
		if newLeaderAck.Cballot > maxCballot {
			maxCballot = newLeaderAck.Cballot
		}
	}

	moreThanFourth := func(cmdId CommandId) *MNewLeaderAck {
		n := 1
		for id0 := 0; id0 < q.size; id0++ {
			e0 := q.elements[id0]
			newLeaderAck0 := e0.(*MNewLeaderAck)
			d := newLeaderAck0.Deps[cmdId]
			for id := 0; id < q.size; id++ {
				e := q.elements[id]

				if id == id0 {
					continue
				}

				newLeaderAck := e.(*MNewLeaderAck)
				d2 := newLeaderAck.Deps[cmdId]

				if d.Equals(d2) {
					n++
				}

				if n > r.N/4 {
					return newLeaderAck0
				}
			}
		}
		return nil
	}

	r.phases = make(map[CommandId]int)
	r.cmds = make(map[CommandId]state.Command)
	r.deps = make(map[CommandId]DepVector)
	r.vectors = make(map[state.Key]*DepVector)
	// TODO: reset committer, gc, and propose ?

	acceptedCmds := make(map[CommandId]struct{})
	builder := newBuilder()

	for i := 0; i < q.size; i++ {
		e := q.elements[i]
		newLeaderAck := e.(*MNewLeaderAck)
		for cmdId, p := range newLeaderAck.Phases {
			_, exists := acceptedCmds[cmdId]
			if exists {
				continue
			}
			if p == COMMIT || p == DELIVER ||
				(p == SLOW_ACCEPT && newLeaderAck.Cballot == maxCballot) {
				if p == SLOW_ACCEPT {
					r.phases[cmdId] = p
				} else {
					r.phases[cmdId] = COMMIT
				}
				r.cmds[cmdId] = newLeaderAck.Cmds[cmdId]
				r.deps[cmdId] = newLeaderAck.Deps[cmdId]
				acceptedCmds[cmdId] = struct{}{}
				builder.adjust(cmdId, r.deps[cmdId])
			} else {
				someMsg := moreThanFourth(cmdId)
				if someMsg != nil {
					r.phases[cmdId] = SLOW_ACCEPT
					r.cmds[cmdId] = someMsg.Cmds[cmdId]
					r.deps[cmdId] = someMsg.Deps[cmdId]
					acceptedCmds[cmdId] = struct{}{}
					builder.adjust(cmdId, r.deps[cmdId])
				}
			}
		}
	}

	nopCmds := make(map[CommandId]struct{})
	for _, dep := range r.deps {
		dep.Iter(func(depCmdId CommandId) bool {
			p, exists := r.phases[depCmdId]
			if !exists || p == START {
				r.phases[depCmdId] = SLOW_ACCEPT
				r.cmds[depCmdId] = state.NOOP()[0]
				nopCmds[depCmdId] = struct{}{}
			}
			return false
		})
	}
	for nopId := range nopCmds {
		r.deps[nopId] = *EmptyVector()
	}

	r.cballot = r.ballot
	r.committer = builder.buildCommitterFrom(r.committer,
		&(r.Mutex), &(r.Shutdown))

	sync := &MSync{
		Replica: r.Id,
		Ballot:  r.ballot,
		Phases:  r.phases,
		Cmds:    r.cmds,
		Deps:    r.deps,
	}
	go func() {
		r.Lock()
		defer r.Unlock()
		r.sendToAll(sync, r.cs.syncRPC)
	}()
}

func (r *Replica) handleSync(msg *MSync) {
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

	r.clean()

	builder := newBuilder()
	for cmdId, dep := range r.deps {
		builder.adjust(cmdId, dep)
	}
	r.committer = builder.buildCommitterFrom(r.committer,
		&(r.Mutex), &(r.Shutdown))

	r.vectors = make(map[state.Key]*DepVector)
	r.updateVectors()

	syncAck := &MSyncAck{
		Replica: r.Id,
		Ballot:  msg.Ballot,
	}
	go r.SendMsg(leader(msg.Ballot, r.N), r.cs.syncAckRPC, syncAck)
}

func (r *Replica) handleSyncAck(msg *MSyncAck) {
	r.Lock()
	defer r.Unlock()

	if r.status != PREPARING || r.ballot != msg.Ballot {
		return
	}

	qs, exists := r.syncAckQuorumSets[msg.Ballot]
	if !exists {
		slowQuorumSize := r.N / 2 // +1 ?

		related := func(e1, e2 interface{}) bool {
			return true
		}

		strongTest := func(q *quorum) bool {
			return q.size >= slowQuorumSize
		}

		weakTest := strongTest

		waitFor := time.Duration(0)

		qs = newQuorumSet(related, strongTest,
			weakTest, r.handleSyncAcks, waitFor)
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

	slowQuorumSize := r.N / 2 // +1 ?

	if q.size < slowQuorumSize {
		return
	}

	someMsg := q.elements[0].(*MSyncAck)
	if r.ballot != someMsg.Ballot {
		return
	}

	r.status = LEADER
	for cmdId, p := range r.phases {
		if p == COMMIT {
			continue
		}

		commit := &MCommit{
			Replica:   r.Id,
			Ballot:    r.ballot,
			CommandId: cmdId,
			Command:   r.cmds[cmdId],
			Dep:       r.deps[cmdId],
		}
		go r.sendToAll(commit, r.cs.commitRPC)
		go r.handleCommit(commit)
	}

	r.clean()
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

	newLeader := &MNewLeader{
		Replica: r.Id,
		Ballot:  newBallot,
	}
	go r.sendToAll(newLeader, r.cs.newLeaderRPC)
	go r.handleNewLeader(newLeader)

	return nil
}

func (r *Replica) executeAndReply(cmdId CommandId) error {
	if r.phases[cmdId] != COMMIT {
		return errors.New("command has not yet been committed")
	}
	r.phases[cmdId] = DELIVER

	if !r.Exec {
		return nil
	}
	cmd := r.cmds[cmdId]
	if cmd.Op == state.NONE {
		// NOOP
		return nil
	}
	dlog.Printf("Executing " + cmd.String())
	v := cmd.Execute(r.State)

	if !r.Dreply {
		return nil
	}

	p, exists := r.proposes[cmdId]
	if !exists {
		return nil
	}

	proposeReply := &genericsmrproto.ProposeReplyTS{
		OK:        genericsmr.TRUE,
		CommandId: p.CommandId,
		Value:     v,
		Timestamp: p.Timestamp,
	}
	r.ReplyProposeTS(proposeReply, p.Reply, p.Mutex)

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

func (r *Replica) add(cmd *state.Command, cmdId CommandId) {
	dep, exists := r.vectors[cmd.K]
	if !exists {
		if cmd.Op != state.PUT {
			return
		}
		dep = &DepVector{
			Size: 0,
			Vect: make([]CommandId, 10),
		}
		r.vectors[cmd.K] = dep
	}

	dep.Add(cmd, cmdId)
}

func (r *Replica) updateVectors() {
	lastNonEmpty := make(map[state.Key]*DepVector)
	r.committer.undeliveredIter(func(cmdId CommandId) {
		key := r.cmds[cmdId].K
		dep := r.deps[cmdId]
		if !dep.IsEmpty() {
			lastNonEmpty[key] = &dep
		}
		r.vectors[key] = &dep
	})

	for key, dep := range lastNonEmpty {
		if r.vectors[key].IsEmpty() {
			r.vectors[key] = dep
		}
	}
}

func (r *Replica) clean() {
	for cmdId, _ := range r.phases {
		if r.committer.wasDelivered(cmdId) {
			r.gc.clean(cmdId)
		}
	}
}

func (r *Replica) flush() {
	cmdId := r.committer.cmdIds[r.committer.last]
	if r.status == LEADER {
		r.committer.deliver(cmdId, r.executeAndReply)
	} else if r.status == FOLLOWER {
		r.committer.safeDeliver(cmdId, r.executeAndReply)
	}
}

func leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func inConflict(c1, c2 state.Command) bool {
	return state.Conflict(&c1, &c2)
}
