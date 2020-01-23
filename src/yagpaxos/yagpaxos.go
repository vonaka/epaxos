package yagpaxos

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"state"
	"sync"
	"time"

	"github.com/orcaman/concurrent-map"
)

type Replica struct {
	*genericsmr.Replica

	ballot  int32
	cballot int32
	status  int

	cmdDescs  cmap.ConcurrentMap
	delivered cmap.ConcurrentMap
	keys      map[state.Key]keyInfo
	keysL     sync.Mutex

	qs quorumSet

	cs CommunicationSupply

	paxosSim bool
}

type commandDesc struct {
	phase   int
	cmd     state.Command
	dep     Dep
	propose *genericsmr.Propose

	fastAndSlowAcks *msgSet

	preAccOrPayloadOnlyCondF *condF
	payloadOnlyCondF         *condF

	msgs   chan interface{}
	active bool

	child     *commandDesc
	childLock sync.Mutex
}

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
	thrifty, exec, lread, dreply, paxosSim bool, failures int) *Replica {

	r := &Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  FOLLOWER,

		cmdDescs:  cmap.New(),
		delivered: cmap.New(),
		keys:      make(map[state.Key]keyInfo),

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

		paxosSim: paxosSim,
	}

	r.qs = newQuorumSet(r.N/2+1, r.N)

	if r.leader() == r.Id {
		r.status = LEADER
	}

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

	return r
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
			cmdId := CommandId{
				ClientId: propose.ClientId,
				SeqNum:   propose.CommandId,
			}
			r.getCmdDesc(cmdId, propose)

		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			r.getCmdDesc(fastAck.CmdId, fastAck)

		case m := <-r.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			r.getCmdDesc(slowAck.CmdId, slowAck)

		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			r.getCmdDesc(lightSlowAck.CmdId, lightSlowAck)

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

func (r *Replica) handlePropose(msg *genericsmr.Propose, desc *commandDesc) {
	WQ := r.WQ()
	cmdId := CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}

	if desc.phase != START || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if !WQ.contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		desc.payloadOnlyCondF.recall()
		return
	}

	if r.paxosSim {
		if r.Id == r.leader() {
			desc.dep = r.getDepAndUpdateInfo(msg.Command, cmdId)
			desc.phase = ACCEPT
		} else {
			desc.dep = []CommandId{
				CommandId{-1, -1},
			}
			desc.phase = PRE_ACCEPT
			desc.preAccOrPayloadOnlyCondF.recall()
			return
		}
	} else {
		desc.dep = r.getDepAndUpdateInfo(msg.Command, cmdId)

		if r.Id == r.leader() {
			desc.phase = ACCEPT
		} else {
			desc.phase = PRE_ACCEPT
			defer desc.preAccOrPayloadOnlyCondF.recall()
		}
	}

	fastAck := &MFastAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		CmdId:   cmdId,
		Dep:     desc.dep,
	}

	go r.sendToAll(fastAck, r.cs.fastAckRPC)
	r.handleFastAck(fastAck, desc)
}

func (r *Replica) handleFastAck(msg *MFastAck, desc *commandDesc) {
	if msg.Replica == r.leader() && r.WQ().contains(r.Id) {
		r.fastAckFromLeaderToWQ(msg, desc)
	} else {
		r.commonCaseFastAck(msg, desc)
	}
}

func (r *Replica) fastAckFromLeaderToWQ(msg *MFastAck, desc *commandDesc) {
	if (r.status != FOLLOWER && r.status != LEADER) ||
		r.ballot != msg.Ballot || desc.phase == COMMIT {
		return
	}

	if r.status == LEADER {
		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
		return
	}

	if desc.phase == ACCEPT {
		return
	}

	desc.preAccOrPayloadOnlyCondF.call(func() {
		if r.status != FOLLOWER || r.ballot != msg.Ballot {
			return
		}

		// TODO: make sure that
		//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}

		desc.phase = ACCEPT

		dep := Dep(msg.Dep)
		equals, _ := desc.dep.EqualsAndDiff(dep)
		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)

		if !equals {
			// TODO: loop at line 35

			desc.dep = dep

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

			go func() {
				r.sendExcept(r.WQ(), slowAck, r.cs.slowAckRPC)
				r.sendTo(r.WQ(), lightSlowAck, r.cs.lightSlowAckRPC)
			}()
			r.handleLightSlowAck(lightSlowAck, desc)
		}
	})
}

func (r *Replica) commonCaseFastAck(msg *MFastAck, desc *commandDesc) {
	if (r.status != FOLLOWER && r.status != LEADER) ||
		r.ballot != msg.Ballot || desc.phase == COMMIT {
		return
	}

	desc.fastAndSlowAcks.add(msg.Replica, r.ballot,
		msg.Replica == r.leader(), msg)
}

func (r *Replica) handleFastAndSlowAcks(leaderMsg interface{},
	msgs []interface{}) {

	if leaderMsg == nil {
		return
	}

	var leaderFastAck *MFastAck
	switch leaderMsg := leaderMsg.(type) {
	case *MFastAck:
		leaderFastAck = leaderMsg
	case *MSlowAck:
		leaderFastAck = (*MFastAck)(leaderMsg)
	}

	if r.status != LEADER && r.status != FOLLOWER {
		return
	}

	WQ := r.WQ()
	if WQ.contains(r.Id) {
		desc := r.getCmdDesc(leaderFastAck.CmdId, nil)

		if desc.phase != ACCEPT || leaderFastAck.Ballot != r.ballot {
			return
		}

		desc.phase = COMMIT

		for _, depCmdId := range desc.dep {
			depDesc := r.getCmdDesc(depCmdId, nil)
			if depDesc == nil {
				continue
			}
			depDesc.childLock.Lock()
			depDesc.child = desc
			depDesc.childLock.Unlock()
		}

		r.deliver(leaderFastAck.CmdId, desc)
	} else {
		if r.status != FOLLOWER {
			return
		}

		desc := r.getCmdDesc(leaderFastAck.CmdId, nil)

		desc.payloadOnlyCondF.call(func() {
			if r.status != FOLLOWER || leaderFastAck.Ballot != r.ballot {
				return
			}

			desc.phase = COMMIT
			desc.dep = leaderFastAck.Dep

			for _, depCmdId := range desc.dep {
				depDesc := r.getCmdDesc(depCmdId, nil)
				if depDesc == nil {
					continue
				}
				depDesc.childLock.Lock()
				depDesc.child = desc
				depDesc.childLock.Unlock()
			}

			r.deliver(leaderFastAck.CmdId, desc)
		})
	}
}

func (r *Replica) handleSlowAck(msg *MSlowAck, desc *commandDesc) {
	r.commonCaseFastAck((*MFastAck)(msg), desc)
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck, desc *commandDesc) {
	if r.qs.WQ(r.ballot).contains(r.Id) {
		r.commonCaseFastAck(&MFastAck{
			Replica: msg.Replica,
			Ballot:  msg.Ballot,
			CmdId:   msg.CmdId,
			Dep:     nil,
		}, desc)
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

func (r *Replica) handleFlush(msg *MFlush) {

}

func (r *Replica) handleCollect(msg *MCollect) {

}

func (r *Replica) deliver(cmdId CommandId, desc *commandDesc) {
	if desc.phase != COMMIT || !r.Exec {
		return
	}

	for _, cmdIdPrime := range desc.dep {
		if !r.delivered.Has(cmdIdPrime.String()) {
			return
		}
	}

	desc.active = false
	desc.msgs <- true

	if isNoop(desc.cmd) {
		r.delivered.Set(cmdId.String(), struct{}{})
		return
	}

	dlog.Printf("Executing " + desc.cmd.String())
	v := desc.cmd.Execute(r.State)

	if !r.Dreply {
		r.delivered.Set(cmdId.String(), struct{}{})
		return
	}

	r.delivered.Set(cmdId.String(), struct{}{})
	if desc.child != nil {
		go func() {
			desc.childLock.Lock()
			defer desc.childLock.Unlock()
			desc.child.msgs <- "deliver"
		}()
	}

	go func() {
		proposeReply := &genericsmrproto.ProposeReplyTS{
			OK:        genericsmr.TRUE,
			CommandId: desc.propose.CommandId,
			Value:     v,
			Timestamp: desc.propose.Timestamp,
		}
		r.ReplyProposeTS(proposeReply, desc.propose.Reply, desc.propose.Mutex)

		r.cmdDescs.Remove(cmdId.String())
	}()
}

func (r *Replica) leader() int32 {
	return leader(r.ballot, r.N)
}

func (r *Replica) WQ() quorum {
	return r.qs.WQ(r.ballot)
}

func (r *Replica) handleDesc(desc *commandDesc) {
	for desc.active {
		switch msg := (<-desc.msgs).(type) {

		case *genericsmr.Propose:
			r.handlePropose(msg, desc)

		case *MFastAck:
			r.handleFastAck(msg, desc)

		case *MSlowAck:
			r.handleSlowAck(msg, desc)

		case *MLightSlowAck:
			r.handleLightSlowAck(msg, desc)

		case string:
			propose := desc.propose
			if propose == nil {
				return
			}
			r.deliver(CommandId{
				ClientId: propose.ClientId,
				SeqNum:   propose.CommandId,
			}, desc)
		}
	}
}

func (r *Replica) getCmdDesc(cmdId CommandId, msg interface{}) *commandDesc {
	if r.delivered.Has(cmdId.String()) {
		return nil
	}

	res := r.cmdDescs.Upsert(cmdId.String(), nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				if msg != nil {
					mapV.(*commandDesc).msgs <- msg
				}

				return mapV
			}

			desc := &commandDesc{
				msgs:   make(chan interface{}, 3),
				active: true,
				phase:  START,
				child:  nil,
			}

			desc.preAccOrPayloadOnlyCondF = newCondF(func() bool {
				return desc.phase == PRE_ACCEPT || desc.phase == PAYLOAD_ONLY
			})

			desc.payloadOnlyCondF = newCondF(func() bool {
				return desc.phase == PAYLOAD_ONLY
			})

			acceptFastAndSlowAck := func(msg interface{}) bool {
				if desc.fastAndSlowAcks.leaderMsg == nil {
					return true
				}

				switch leaderMsg := desc.fastAndSlowAcks.leaderMsg.(type) {
				case *MFastAck:
					return msg.(*MFastAck).Dep == nil ||
						(Dep(leaderMsg.Dep)).Equals(msg.(*MFastAck).Dep)
				default:
					return false
				}
			}

			desc.fastAndSlowAcks =
				newMsgSet(r.WQ(), acceptFastAndSlowAck, r.handleFastAndSlowAcks)

			go r.handleDesc(desc)

			if msg != nil {
				desc.msgs <- msg
			}

			return desc
		})

	return res.(*commandDesc)
}

func (r *Replica) getDepAndUpdateInfo(cmd state.Command, cmdId CommandId) Dep {
	r.keysL.Lock()
	defer r.keysL.Unlock()

	dep := []CommandId{}
	keysOfCmd := keysOf(cmd)

	for _, key := range keysOfCmd {
		info, exists := r.keys[key]

		if exists {
			cdep := info.getConflictCmds(cmd)
			dep = append(dep, cdep...)
		} else {
			info = newLightKeyInfo()
			r.keys[cmd.K] = info
		}

		info.add(cmd, cmdId)
	}

	return dep
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

func (r *Replica) sendTo(q quorum, msg fastrpc.Serializable, rpc uint8) {
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
}

func (r *Replica) sendExcept(q quorum, msg fastrpc.Serializable, rpc uint8) {
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
}
