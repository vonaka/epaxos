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

	cmdDescs map[CommandId]*CommandDesc
	vectors  map[state.Key]*superDepVector
	buf      map[CommandId]int

	committer *committer
	gc        *gc

	ignoreCommitted bool

	newLeaderAckQuorumSets map[int32]*quorumSet
	syncAckQuorumSets      map[int32]*quorumSet

	cs CommunicationSupply
}

type CommandDesc struct {
	phase   int
	cmd     state.Command
	dep     DepVector
	propose *genericsmr.Propose

	fastAckQuorumSet *quorumSet
	slowAckQuorumSet *quorumSet
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
	flushChan        chan fastrpc.Serializable

	fastAckRPC      uint8
	commitRPC       uint8
	slowAckRPC      uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	syncRPC         uint8
	syncAckRPC      uint8
	collectRPC      uint8
	flushRPC        uint8
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply, ignoreCommitted bool) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply),

		status:  FOLLOWER,
		ballot:  0,
		cballot: 0,

		cmdDescs: make(map[CommandId]*CommandDesc),
		vectors:  make(map[state.Key]*superDepVector),

		ignoreCommitted: ignoreCommitted,

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
			flushChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
		},
	}
	r.committer = newCommitter(&(r.Mutex), &(r.Shutdown))
	r.gc = newGc(func(cmdId CommandId) {
		_, exists := r.cmdDescs[cmdId]
		if exists {
			delete(r.cmdDescs, cmdId)
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
	r.cs.flushRPC =
		r.RegisterRPC(new(MFlush), r.cs.flushChan)

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
		case m := <-r.cs.flushChan:
			flush := m.(*MFlush)
			go r.handleFlush(flush)
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

	desc, exists := r.cmdDescs[cmdId]
	if !exists {
		desc = &CommandDesc{}
		r.cmdDescs[cmdId] = desc
	}
	if desc.propose != nil {
		return
	}

	if desc.phase < SLOW_ACCEPT {
		desc.phase = FAST_ACCEPT
		desc.cmd = msg.Command
		dep, exists := r.vectors[msg.Command.K]
		if exists {
			desc.dep = dep.vector
		} else {
			desc.dep = *EmptyVector()
		}
		r.add(&msg.Command, cmdId)
	}

	desc.propose = msg

	fastAck := &MFastAck{
		Replica:   r.Id,
		Ballot:    r.ballot,
		CommandId: cmdId,
		Command:   msg.Command,
		Dep:       desc.dep,
	}
	if r.status == LEADER {
		r.committer.add(cmdId)
		fastAck.AcceptId = r.committer.getInstance(cmdId)
	} else {
		fastAck.AcceptId = -1
	}
	r.sendToAll(fastAck, r.cs.fastAckRPC)
	go r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *MFastAck) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || r.ballot != msg.Ballot {
		return
	}

	desc, exists := r.cmdDescs[msg.CommandId]
	if !exists {
		desc = &CommandDesc{}
		r.cmdDescs[msg.CommandId] = desc
	}

	if desc.fastAckQuorumSet == nil {
		fastQuorumSize := 3*r.N/4 + 1
		slowQuorumSize := r.N/2 + 1

		related := func(e1, e2 interface{}) bool {
			fastAck1 := e1.(*MFastAck)
			fastAck2 := e2.(*MFastAck)
			return fastAck1.Dep.Equals(&(fastAck2.Dep))
		}

		totalNum := 0
		rest := r.N - fastQuorumSize
		strongTest := func(q *quorum) bool {
			totalNum++
			qs := desc.fastAckQuorumSet
			if q.size < fastQuorumSize && totalNum > rest {
				qs.afterHours = true
				if qs.weakTest(qs.mainQuorum) {
					go qs.handler(qs.mainQuorum)
				}
				return false
			}
			return q.size >= fastQuorumSize
		}

		weakTest := func(q *quorum) bool {
			return q.size >= slowQuorumSize
		}

		waitFor := time.Duration(r.N+1) * r.cs.maxLatency // FIXME

		desc.fastAckQuorumSet = newQuorumSet(related, strongTest,
			weakTest, r.handleFastAcks, waitFor)
	}

	fromLeader := msg.Replica == leader(r.ballot, r.N)
	if fromLeader && r.status == FOLLOWER {
		r.committer.addTo(msg.CommandId, msg.AcceptId)
	}
	desc.fastAckQuorumSet.add(msg, fromLeader)
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

		if r.status == LEADER {
			r.sendToAll(commit, r.cs.commitRPC)
		}
		go r.handleCommit(commit)
	} else {
		desc, exists := r.cmdDescs[leaderFastAck.CommandId]
		if !exists {
			desc = &CommandDesc{}
			r.cmdDescs[leaderFastAck.CommandId] = desc
		}
		desc.phase = SLOW_ACCEPT
		if r.status == FOLLOWER {
			desc.cmd = leaderFastAck.Command
			desc.dep = leaderFastAck.Dep
		}

		slowAck := &MSlowAck{
			Replica:   r.Id,
			Ballot:    leaderFastAck.Ballot,
			CommandId: leaderFastAck.CommandId,
			Command:   leaderFastAck.Command,
			Dep:       leaderFastAck.Dep,
		}
		r.sendToAll(slowAck, r.cs.slowAckRPC)
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

	desc, exists := r.cmdDescs[msg.CommandId]
	if !exists {
		desc = &CommandDesc{}
		r.cmdDescs[msg.CommandId] = desc
	}

	if (r.status != LEADER && r.status != FOLLOWER) || msg.Ballot > r.ballot ||
		desc.phase == DELIVER {
		return
	}

	desc.phase = COMMIT
	desc.cmd = msg.Command
	desc.dep = msg.Dep

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
		r.sendToAll(collect, r.cs.collectRPC)
		go r.handleCollect(collect)
	}
}

func (r *Replica) handleSlowAck(msg *MSlowAck) {
	r.Lock()
	defer r.Unlock()

	if (r.status != LEADER && r.status != FOLLOWER) || r.ballot != msg.Ballot {
		return
	}

	desc, exists := r.cmdDescs[msg.CommandId]
	if !exists {
		desc = &CommandDesc{}
		r.cmdDescs[msg.CommandId] = desc
	}

	if desc.slowAckQuorumSet == nil {
		slowQuorumSize := r.N/2 + 1

		related := func(e1, e2 interface{}) bool {
			return true
		}

		strongTest := func(q *quorum) bool {
			return q.size >= slowQuorumSize
		}

		weakTest := strongTest

		waitFor := time.Duration(0)

		desc.slowAckQuorumSet = newQuorumSet(related, strongTest,
			weakTest, r.handleSlowAcks, waitFor)
	}

	desc.slowAckQuorumSet.add(msg, msg.Replica == leader(r.ballot, r.N))
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

	// TODO: get rid of it
	phases := make(map[CommandId]int)
	cmds := make(map[CommandId]state.Command)
	deps := make(map[CommandId]DepVector)

	for cmdId, desc := range r.cmdDescs {
		phases[cmdId] = desc.phase
		cmds[cmdId] = desc.cmd
		deps[cmdId] = desc.dep
	}

	newLeaderAck := &MNewLeaderAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cballot: r.cballot,
		Phases:  phases,
		Cmds:    cmds,
		Deps:    deps,
	}

	if l := leader(r.ballot, r.N); l == r.Id {
		go r.handleNewLeaderAck(newLeaderAck)
	} else {
		r.SendMsg(l, r.cs.newLeaderAckRPC, newLeaderAck)
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

				if d.Equals(&d2) {
					n++
				}

				if n > r.N/4 {
					return newLeaderAck0
				}
			}
		}
		return nil
	}

	newCmdDescs := make(map[CommandId]*CommandDesc)
	r.vectors = make(map[state.Key]*superDepVector)

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
				desc := &CommandDesc{}
				newCmdDescs[cmdId] = desc
				if p == SLOW_ACCEPT {
					desc.phase = p
				} else {
					desc.phase = COMMIT
				}
				desc.cmd = newLeaderAck.Cmds[cmdId]
				desc.dep = newLeaderAck.Deps[cmdId]
				oldDesc, exists := r.cmdDescs[cmdId]
				if exists {
					desc.propose = oldDesc.propose
				}
				acceptedCmds[cmdId] = struct{}{}
				builder.adjust(cmdId, desc.dep)
			} else {
				someMsg := moreThanFourth(cmdId)
				if someMsg != nil {
					desc := &CommandDesc{}
					newCmdDescs[cmdId] = desc
					desc.phase = SLOW_ACCEPT
					desc.cmd = someMsg.Cmds[cmdId]
					desc.dep = someMsg.Deps[cmdId]
					oldDesc, exists := r.cmdDescs[cmdId]
					if exists {
						desc.propose = oldDesc.propose
					}
					acceptedCmds[cmdId] = struct{}{}
					builder.adjust(cmdId, desc.dep)
				}
			}
		}
	}

	r.cmdDescs = newCmdDescs

	nopCmds := make(map[CommandId]struct{})
	for _, desc := range r.cmdDescs {
		desc.dep.Iter(func(depCmdId CommandId) bool {
			depDesc, exists := r.cmdDescs[depCmdId]
			if !exists {
				depDesc = &CommandDesc{}
				r.cmdDescs[depCmdId] = depDesc
			}
			if depDesc.phase == START {
				depDesc.phase = SLOW_ACCEPT
				depDesc.cmd = state.NOOP()[0]
				nopCmds[depCmdId] = struct{}{}
			}
			return false
		})
	}
	for nopId := range nopCmds {
		r.cmdDescs[nopId].dep = *EmptyVector()
	}

	r.cballot = r.ballot
	r.committer = builder.buildCommitterFrom(r.committer,
		&(r.Mutex), &(r.Shutdown))

	// TODO: get rid of it
	r.buf = make(map[CommandId]int)
	cmds := make(map[CommandId]state.Command)
	deps := make(map[CommandId]DepVector)

	for cmdId, desc := range r.cmdDescs {
		r.buf[cmdId] = desc.phase
		cmds[cmdId] = desc.cmd
		deps[cmdId] = desc.dep
	}

	sync := &MSync{
		Replica: r.Id,
		Ballot:  r.ballot,
		Phases:  r.buf,
		Cmds:    cmds,
		Deps:    deps,
	}
	r.sendToAll(sync, r.cs.syncRPC)
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
	r.buf = msg.Phases

	newCmdDescs := make(map[CommandId]*CommandDesc)
	for cmdId := range msg.Phases {
		desc := &CommandDesc{}
		newCmdDescs[cmdId] = desc
		desc.phase = msg.Phases[cmdId]
		desc.cmd = msg.Cmds[cmdId]
		desc.dep = msg.Deps[cmdId]
		oldDesc, exists := r.cmdDescs[cmdId]
		if exists {
			desc.propose = oldDesc.propose
		}
	}
	r.cmdDescs = newCmdDescs

	r.clean()

	builder := newBuilder()
	for cmdId, desc := range r.cmdDescs {
		builder.adjust(cmdId, desc.dep)
	}
	r.committer = builder.buildCommitterFrom(r.committer,
		&(r.Mutex), &(r.Shutdown))

	r.vectors = make(map[state.Key]*superDepVector)
	r.updateVectors()

	syncAck := &MSyncAck{
		Replica: r.Id,
		Ballot:  msg.Ballot,
	}
	r.SendMsg(leader(msg.Ballot, r.N), r.cs.syncAckRPC, syncAck)
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
	r.sendToAll(&MFlush{}, r.cs.flushRPC)
	go r.handleFlush(&MFlush{})

	r.clean()
	r.updateVectors()
}

func (r *Replica) handleFlush(*MFlush) {
	r.Lock()
	defer r.Unlock()

	for cmdId, phase := range r.buf {
		if phase == COMMIT {
			continue
		}

		desc, exists := r.cmdDescs[cmdId]
		if !exists {
			continue
		}

		commit := &MCommit{
			Replica:   r.Id,
			Ballot:    r.ballot,
			CommandId: cmdId,
			Command:   desc.cmd,
			Dep:       desc.dep,
		}
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

	newLeader := &MNewLeader{
		Replica: r.Id,
		Ballot:  newBallot,
	}
	r.sendToAll(newLeader, r.cs.newLeaderRPC)
	go r.handleNewLeader(newLeader)

	return nil
}

func (r *Replica) executeAndReply(cmdId CommandId) error {
	desc, exists := r.cmdDescs[cmdId]

	if !exists || desc.phase != COMMIT {
		return errors.New("command has not yet been committed")
	}
	desc.phase = DELIVER

	if !r.Exec {
		return nil
	}
	cmd := desc.cmd
	if cmd.Op == state.NONE {
		// NOOP
		return nil
	}
	dlog.Printf("Executing " + cmd.String())
	v := cmd.Execute(r.State)

	if !r.Dreply {
		return nil
	}

	if desc.propose == nil {
		return nil
	}

	proposeReply := &genericsmrproto.ProposeReplyTS{
		OK:        genericsmr.TRUE,
		CommandId: desc.propose.CommandId,
		Value:     v,
		Timestamp: desc.propose.Timestamp,
	}
	r.ReplyProposeTS(proposeReply, desc.propose.Reply, desc.propose.Mutex)

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
		dep = newSuperDepVector()
		r.vectors[cmd.K] = dep
	}

	dep.add(cmd, cmdId)
}

func (r *Replica) updateVectors() {
	r.committer.undeliveredIter(func(cmdId CommandId) {
		cmd := r.cmdDescs[cmdId].cmd
		r.add(&cmd, cmdId)
	})
}

func (r *Replica) clean() {
	for cmdId := range r.cmdDescs {
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

type superDepVector struct {
	vector         DepVector
	commandIndices map[int32]int
}

func newSuperDepVector() *superDepVector {
	return &superDepVector{
		vector: DepVector{
			Size: 0,
			Vect: make([]CommandId, 10),
		},
		commandIndices: make(map[int32]int, 10),
	}
}

func (sdv *superDepVector) add(cmd *state.Command, cmdId CommandId) {
	if cmd.Op != state.PUT {
		return
	}

	cmdIndex, exists := sdv.commandIndices[cmdId.ClientId]
	vector := &(sdv.vector)
	if exists {
		vector.Vect[cmdIndex] = cmdId
	} else {
		if vector.Size < len(vector.Vect) {
			vector.Vect[vector.Size] = cmdId
		} else {
			vector.Vect = append(vector.Vect, cmdId)
		}
		sdv.commandIndices[cmdId.ClientId] = vector.Size
		vector.Size++
	}
}
