package yagpaxos

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"state"
	"time"
)

type Replica struct {
	*genericsmr.Replica

	ballot  int32
	cballot int32
	status  int
	qs      quorumSet
	gc      *gc

	cmdDescs map[CommandId]*commandDesc
	keysInfo map[state.Key]*keyInfo

	cs CommunicationSupply

	proposeProbe           *probe
	fastAckFromLeaderProbe *probe
	fastAckProbe           *probe
	quorumAddProb          *probe
	//slowAckProbe           *probe
	//lightSlowAckProbe      *probe
}

type commandDesc struct {
	phase   int
	cmd     state.Command
	dep     Dep
	propose *genericsmr.Propose

	fastAndSlowAcks *msgSet

	delivered bool

	preAccOrPayloadOnlyCondF *condF
	payloadOnlyCondF         *condF
	depDeliverCondF          *condF

	// we suppose that the maximal dep size is 1
	parentId CommandId
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
	PAYLOAD_ONLY
	PRE_ACCEPT
	ACCEPT
	COMMIT
)

type CommunicationSupply struct {
	maxLatency time.Duration

	fastAckChan      chan fastrpc.Serializable
	slowAckChan      chan fastrpc.Serializable
	lightSlowAckChan chan fastrpc.Serializable
	newLeaderChan    chan fastrpc.Serializable
	newLeaderAckChan chan fastrpc.Serializable
	syncChan         chan fastrpc.Serializable
	syncAckChan      chan fastrpc.Serializable
	flushChan        chan fastrpc.Serializable
	collectChan      chan fastrpc.Serializable

	fastAckRPC      uint8
	slowAckRPC      uint8
	lightSlowAckRPC uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	syncRPC         uint8
	syncAckRPC      uint8
	flushRPC        uint8
	collectRPC      uint8
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply bool, failures int) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  FOLLOWER,

		cmdDescs: make(map[CommandId]*commandDesc),
		keysInfo: make(map[state.Key]*keyInfo),

		cs: CommunicationSupply{
			maxLatency: 0,

			fastAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			slowAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			lightSlowAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			newLeaderChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			newLeaderAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			syncChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			syncAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			flushChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			collectChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
		},

		proposeProbe:           newProbe("propose"),
		fastAckFromLeaderProbe: newProbe("FastAck from leader"),
		fastAckProbe:           newProbe("FastAck"),
		quorumAddProb:          newProbe("quorum add"),
		//slowAckProbe:           newProbe("SlowAck"),
		//lightSlowAckProbe:      newProbe("lightSlowAck"),
	}

	if r.leader() == r.Id {
		r.status = LEADER
	}

	r.qs = newQuorumSet(r.N/2+1, r.N)

	/*
	r.gc = newGc(func(cmdId CommandId) {
		desc, exists := r.cmdDescs[cmdId]
		if exists {
			//desc.Broadcast()
			desc.deliverCond.Broadcast()
			desc.phaseCond.Broadcast()
			cmd := desc.cmd
			ki, exists := r.keysInfo[cmd.K]
			if exists {
				ki.remove(cmdId)
				delete(r.cmdDescs, cmdId)
			}
		}
	}, &r.Mutex, &r.Shutdown)*/

	r.cs.fastAckRPC =
		r.RegisterRPC(new(MFastAck), r.cs.fastAckChan)
	r.cs.slowAckRPC =
		r.RegisterRPC(new(MSlowAck), r.cs.slowAckChan)
	r.cs.lightSlowAckRPC =
		r.RegisterRPC(new(MLightSlowAck), r.cs.lightSlowAckChan)
	r.cs.newLeaderRPC =
		r.RegisterRPC(new(MNewLeader), r.cs.newLeaderChan)
	r.cs.newLeaderAckRPC =
		r.RegisterRPC(new(MNewLeaderAck), r.cs.newLeaderAckChan)
	r.cs.syncRPC =
		r.RegisterRPC(new(MSync), r.cs.syncChan)
	r.cs.syncAckRPC =
		r.RegisterRPC(new(MSyncAck), r.cs.syncAckChan)
	r.cs.flushRPC =
		r.RegisterRPC(new(MFlush), r.cs.flushChan)
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

	for !r.Shutdown {
		select {

		case propose := <-r.ProposeChan:
			r.handlePropose(propose)

		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			r.handleFastAck(fastAck)

		case m := <-r.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			r.handleSlowAck(slowAck)

		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			r.handleLightSlowAck(lightSlowAck)

		case m := <-r.cs.newLeaderChan:
			newLeader := m.(*MNewLeader)
			r.handleNewLeader(newLeader)

		case m := <-r.cs.newLeaderAckChan:
			newLeaderAck := m.(*MNewLeaderAck)
			r.handleNewLeaderAck(newLeaderAck)

		case m := <-r.cs.syncChan:
			sync := m.(*MSync)
			r.handleSync(sync)

		case m := <-r.cs.syncAckChan:
			syncAck := m.(*MSyncAck)
			r.handleSyncAck(syncAck)

		case m := <-r.cs.flushChan:
			flush := m.(*MFlush)
			r.handleFlush(flush)

		case m := <-r.cs.collectChan:
			collect := m.(*MCollect)
			r.handleCollect(collect)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	r.proposeProbe.start()

	WQ := r.qs.WQ(r.ballot)
	cmdId := CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}
	desc := r.getCmdDesc(cmdId)

	if desc.propose != nil {
		r.proposeProbe.stop()
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if !WQ.contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		r.proposeProbe.stop()
		desc.preAccOrPayloadOnlyCondF.recall()
		desc.payloadOnlyCondF.recall()
		return
	}

	if r.Id == r.leader() {
		desc.phase = ACCEPT
	} else {
		desc.phase = PRE_ACCEPT
		defer func() {
			desc.preAccOrPayloadOnlyCondF.recall()
		}()
	}

	desc.dep = r.generateDepOf(desc.cmd, cmdId)
	r.addCmdInfo(desc.cmd, cmdId)

	if len(desc.dep) > 0 {
		r.getCmdDesc(desc.dep[0]).parentId = cmdId
	}

	fastAck := &MFastAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		CmdId:   cmdId,
		Dep:     desc.dep,
	}

	r.proposeProbe.stop()

	r.sendToAll(fastAck, r.cs.fastAckRPC)
	r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *MFastAck) {
	if msg.Replica == r.leader() && r.qs.WQ(r.ballot).contains(r.Id) {
		r.fastAckFromLeaderToWQ(msg)
	} else {
		r.commonCaseFastAck(msg)
	}
}

func (r *Replica) fastAckFromLeaderToWQ(msg *MFastAck) {
	if (r.status != LEADER && r.status != FOLLOWER) || r.ballot != msg.Ballot {
		return
	}

	// TODO: make sure that
	//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}

	desc := r.getCmdDesc(msg.CmdId)

	if r.status == LEADER {
		r.quorumAddProb.start()
		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
		r.quorumAddProb.stop()
		return
	}

	desc.preAccOrPayloadOnlyCondF.call(func() {
		r.fastAckFromLeaderProbe.start()

		if r.status != FOLLOWER || r.ballot != msg.Ballot {
			r.fastAckFromLeaderProbe.stop()
			return
		}

		desc.phase = ACCEPT

		dep := Dep(msg.Dep)
		equals, diff := desc.dep.EqualsAndDiff(dep)
		if !equals {
			for cmdIdPrime := range diff {
				descPrime := r.getCmdDesc(cmdIdPrime)
				descPrime.phase = PAYLOAD_ONLY
				defer func() {
					descPrime.preAccOrPayloadOnlyCondF.recall()
					descPrime.payloadOnlyCondF.recall()
				}()
			}
			desc.dep = dep

			if len(desc.dep) > 0 {
				r.getCmdDesc(desc.dep[0]).parentId = msg.CmdId
			}

			slowAck := &MSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   msg.CmdId,
				Dep:     desc.dep,
			}

			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   msg.CmdId,
			}

			r.fastAckFromLeaderProbe.stop()

			r.quorumAddProb.start()
			desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
			r.quorumAddProb.stop()

			r.sendExcept(r.qs.WQ(r.ballot), slowAck, r.cs.slowAckRPC)
			r.sendTo(r.qs.WQ(r.ballot), lightSlowAck, r.cs.lightSlowAckRPC)
			r.handleLightSlowAck(lightSlowAck)
		} else {
			r.fastAckFromLeaderProbe.stop()
			r.quorumAddProb.start()
			desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
			r.quorumAddProb.stop()
		}
	})
}

func (r *Replica) commonCaseFastAck(msg *MFastAck) {
	r.fastAckProbe.start()

	if (r.status != FOLLOWER && r.status != LEADER) || r.ballot != msg.Ballot {
		r.fastAckProbe.stop()
		return
	}

	desc := r.getCmdDesc(msg.CmdId)
	if desc.phase == COMMIT {
		r.fastAckProbe.stop()
		return
	}

	r.fastAckProbe.stop()

	r.quorumAddProb.start()
	desc.fastAndSlowAcks.add(msg.Replica, r.ballot,
		msg.Replica == r.leader(), msg)
	r.quorumAddProb.stop()
}

func (r *Replica) handleSlowAck(msg *MSlowAck) {
	r.commonCaseFastAck((*MFastAck)(msg))
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck) {
	if r.qs.WQ(r.ballot).contains(r.Id) {
		r.commonCaseFastAck(&MFastAck{
			Replica: msg.Replica,
			Ballot:  msg.Ballot,
			CmdId:   msg.CmdId,
			Dep:     nil,
		})
	}
}

func (r *Replica) handleFastAndSlowAcks(leaderMsg interface{},
	msgs []interface{}) {
	if leaderMsg == nil || len(msgs) < r.N/2 || r.N == 0 {
		return
	}

	var leaderFastAck *MFastAck
	switch leaderMsg := leaderMsg.(type) {
	case *MFastAck:
		leaderFastAck = leaderMsg
	case *MSlowAck:
		leaderFastAck = (*MFastAck)(leaderMsg)
	}

	if (r.status != LEADER && r.status != FOLLOWER) ||
		r.ballot != leaderFastAck.Ballot {
		return
	}

	WQ := r.qs.WQ(r.ballot)
	desc := r.getCmdDesc(leaderFastAck.CmdId)

	if WQ.contains(r.Id) && desc.phase == ACCEPT {
		desc.phase = COMMIT
		r.deliver(desc)
		return
	} else if !WQ.contains(r.Id) && desc.phase != COMMIT {
		desc.payloadOnlyCondF.call(func() {
			if r.status != FOLLOWER || leaderFastAck.Ballot != r.ballot ||
				desc.phase == COMMIT {
				return
			}

			desc.phase = COMMIT
			desc.dep = leaderFastAck.Dep

			if len(desc.dep) > 0 {
				r.getCmdDesc(desc.dep[0]).parentId = leaderFastAck.CmdId
			}

			r.deliver(desc)
		})
	}
}

func (r *Replica) handleNewLeader(msg *MNewLeader) {

}

func (r *Replica) handleNewLeaderAck(msg *MNewLeaderAck) {

}

func (r *Replica) handleSync(msg *MSync) {

}

func (r *Replica) handleSyncAck(msg *MSyncAck) {

}

func (r *Replica) handleFlush(*MFlush) {

}

func (r *Replica) handleCollect(msg *MCollect) {
	r.gc.collect(msg.CmdId, msg.Replica, r.N)
}

func (r *Replica) leader() int32 {
	return leader(r.ballot, r.N)
}

func (r *Replica) getCmdDesc(cmdId CommandId) *commandDesc {
	WQ := r.qs.WQ(r.ballot)
	desc, exists := r.cmdDescs[cmdId]

	if !exists {
		desc = &commandDesc{
			delivered: false,
			parentId:  NullCmdId,

			preAccOrPayloadOnlyCondF: newCondF(func() bool {
				return desc.phase == PRE_ACCEPT || desc.phase == PAYLOAD_ONLY
			}),

			payloadOnlyCondF: newCondF(func() bool {
				return desc.phase == PAYLOAD_ONLY
			}),

			depDeliverCondF: newCondF(func () bool {
				return true
			}),
		}

		acceptFastOrSlowAck := func(msg interface{}) bool {
			if desc.fastAndSlowAcks.leaderMsg == nil {
				return true
			}
			switch leaderMsg := desc.fastAndSlowAcks.leaderMsg.(type) {
			case *MFastAck:
				return msg.(*MFastAck).Dep == nil ||
					(Dep(leaderMsg.Dep)).Equals(msg.(*MFastAck).Dep)
			}

			return false
		}

		desc.fastAndSlowAcks =
			newMsgSet(WQ, acceptFastOrSlowAck, r.handleFastAndSlowAcks)

		r.cmdDescs[cmdId] = desc
	}

	return desc
}

func (r *Replica) deliver(desc *commandDesc) {
	if desc.phase != COMMIT || desc.delivered || !r.Exec {
		return
	}

	for _, cmdIdPrime := range desc.dep {
		descPrime := r.getCmdDesc(cmdIdPrime)
		desc.depDeliverCondF.andCond(func () bool {
			return descPrime.delivered
		})
	}

	desc.depDeliverCondF.call(func() {
		desc.delivered = true
		defer func() {
			if desc.parentId != NullCmdId {
				r.getCmdDesc(desc.parentId).depDeliverCondF.recall()
			}
		}()

		/*defer func() {
			collect := &MCollect{
				Replica: r.Id,
				CmdId:   CommandId{
					ClientId: desc.propose.ClientId,
					SeqNum:   desc.propose.CommandId,
				},
			}
			r.sendToAll(collect, r.cs.collectRPC)
			r.handleCollect(collect)
		}()*/

		if desc.cmd.Op == state.NONE {
			return
		}

		dlog.Printf("Executing " + desc.cmd.String())
		v := desc.cmd.Execute(r.State)

		if !r.Dreply {
			return
		}

		proposeReply := &genericsmrproto.ProposeReplyTS{
			OK:        genericsmr.TRUE,
			CommandId: desc.propose.CommandId,
			Value:     v,
			Timestamp: desc.propose.Timestamp,
		}
		go r.ReplyProposeTS(proposeReply,
			desc.propose.Reply, desc.propose.Mutex)
	})
}

func (r *Replica) generateDepOf(cmd state.Command, cmdId CommandId) Dep {
	info, exists := r.keysInfo[cmd.K]

	if exists {
		return info.getDep()
	}

	return []CommandId{}
}

func (r *Replica) addCmdInfo(cmd state.Command, cmdId CommandId) {
	info, exists := r.keysInfo[cmd.K]

	if !exists {
		info = newKeyInfo()
		r.keysInfo[cmd.K] = info
	}

	info.add(cmdId)
}

func (r *Replica) sendToAll(msg fastrpc.Serializable, rpc uint8) {
	go func() {
		for p := int32(0); p < int32(r.N); p++ {
			r.M.Lock()
			if r.Alive[p] {
				r.M.Unlock()
				r.SendMsg(p, rpc, msg)
				r.M.Lock()
			}
			r.M.Unlock()
		}
	}()
}

func (r *Replica) sendTo(q quorum, msg fastrpc.Serializable, rpc uint8) {
	go func() {
		for p := int32(0); p < int32(r.N); p++ {
			if !q.contains(p) {
				continue
			}
			r.M.Lock()
			if r.Alive[p] {
				r.M.Unlock()
				r.SendMsg(p, rpc, msg)
				r.M.Lock()
			}
			r.M.Unlock()
		}
	}()
}

func (r *Replica) sendExcept(q quorum, msg fastrpc.Serializable, rpc uint8) {
	go func() {
		for p := int32(0); p < int32(r.N); p++ {
			if q.contains(p) {
				continue
			}
			r.M.Lock()
			if r.Alive[p] {
				r.M.Unlock()
				r.SendMsg(p, rpc, msg)
				r.M.Lock()
			}
			r.M.Unlock()
		}
	}()
}
