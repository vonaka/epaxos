package yagpaxos

import (
	"dlog"
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

	ballot  int32
	cballot int32
	status  int
	qs      quorumSet

	cmdDescs  map[CommandId]*commandDesc
	keysInfo  map[state.Key]*keyInfo

	cs CommunicationSupply
}

type commandDesc struct {
	sync.Mutex

	phase   int
	cmd     state.Command
	dep     Dep
	propose *genericsmr.Propose

	cond            *sync.Cond
	fastAndSlowAcks *msgSet

	deliver   func()
	delivered bool
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

	fastAckRPC      uint8
	slowAckRPC      uint8
	lightSlowAckRPC uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	syncRPC         uint8
	syncAckRPC      uint8
	flushRPC        uint8
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply bool, failures int) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  FOLLOWER,

		cmdDescs:  make(map[CommandId]*commandDesc),
		keysInfo:  make(map[state.Key]*keyInfo),

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
		},
	}

	if r.leader() == r.Id {
		r.status = LEADER
	}

	r.qs = newQuorumSet(r.N/2+1, r.N)

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
			fastAck := m.(*MFastAck)
			go r.handleFastAck(fastAck)

		case m := <-r.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			go r.handleSlowAck(slowAck)

		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			go r.handleLightSlowAck(lightSlowAck)

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

		case m := <-r.cs.flushChan:
			flush := m.(*MFlush)
			go r.handleFlush(flush)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	r.Lock()

	WQ := r.qs.WQ(r.ballot)

	cmdId := CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}

	desc := r.getCmdDesc(cmdId)
	if desc.propose != nil {
		r.Unlock()
		return
	}

	desc.propose = msg
	desc.cond.Broadcast()
	desc.cmd = msg.Command

	if !WQ.contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		r.Unlock()
		return
	}

	if r.Id == r.leader() {
		desc.phase = ACCEPT
	} else {
		desc.phase = PRE_ACCEPT
	}

	desc.dep = r.generateDepOf(desc.cmd, cmdId)
	r.addCmdInfo(desc.cmd, cmdId)

	fastAck := &MFastAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		CmdId:   cmdId,
		Dep:     desc.dep,
	}

	r.sendToAll(fastAck, r.cs.fastAckRPC)
	r.Unlock()
	r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *MFastAck) {
	r.Lock()

	if msg.Replica == r.leader() && r.qs.WQ(r.ballot).contains(r.Id) {
		r.Unlock()
		r.fastAckFromLeaderToWQ(msg)
	} else {
		r.Unlock()
		r.commonCaseFastAck(msg)
	}
}

func (r *Replica) fastAckFromLeaderToWQ(msg *MFastAck) {
	r.Lock()

	if (r.status != FOLLOWER && r.status != LEADER) || r.ballot != msg.Ballot {
		r.Unlock()
		return
	}

	desc := r.getCmdDesc(msg.CmdId)

	if r.status == LEADER {
		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
		r.Unlock()
		return
	}

	if desc.phase == ACCEPT || desc.phase == COMMIT {
		r.Unlock()
		return
	}

	for desc.phase != PRE_ACCEPT && desc.phase != PAYLOAD_ONLY {
		desc.cond.Wait()
		if r.status != FOLLOWER || r.ballot != msg.Ballot ||
			desc.phase == ACCEPT || desc.phase == COMMIT {
			r.Unlock()
			return
		}
	}

	// TODO: make sure that
	//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}

	desc.phase = ACCEPT
	desc.cond.Broadcast()

	dep := Dep(msg.Dep)
	equals, diff := desc.dep.EqualsAndDiff(dep)
	if !equals {
		for cmdIdPrime := range diff {
			descPrime := r.getCmdDesc(cmdIdPrime)
			descPrime.phase = PAYLOAD_ONLY
			descPrime.cond.Broadcast()
		}
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

		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)

		r.sendExcept(r.qs.WQ(r.ballot), slowAck, r.cs.slowAckRPC)
		r.sendTo(r.qs.WQ(r.ballot), lightSlowAck, r.cs.lightSlowAckRPC)
		r.Unlock()
		r.handleLightSlowAck(lightSlowAck)

		return
	}

	desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
	r.Unlock()
}

func (r *Replica) commonCaseFastAck(msg *MFastAck) {
	r.Lock()
	defer r.Unlock()

	if (r.status != FOLLOWER && r.status != LEADER) || r.ballot != msg.Ballot {
		return
	}

	desc := r.getCmdDesc(msg.CmdId)
	if desc.phase == COMMIT {
		return
	}

	desc.fastAndSlowAcks.add(msg.Replica, r.ballot,
		msg.Replica == r.leader(), msg)
}

func (r *Replica) handleSlowAck(msg *MSlowAck) {
	r.commonCaseFastAck((*MFastAck)(msg))
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck) {
	r.Lock()

	if r.qs.WQ(r.ballot).contains(r.Id) {
		r.Unlock()
		r.commonCaseFastAck(&MFastAck{
			Replica: msg.Replica,
			Ballot:  msg.Ballot,
			CmdId:   msg.CmdId,
			Dep:     nil,
		})

		return
	}

	r.Unlock()
}

func (r *Replica) handleFastAndSlowAcks(leaderMsg interface{},
	msgs []interface{}) {
	r.Lock()
	defer r.Unlock()

	if leaderMsg == nil || len(msgs) != r.N/2 || r.N == 0 {
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

	WQ := r.qs.WQ(r.ballot)
	if WQ.contains(r.Id) {
		desc := r.getCmdDesc(leaderFastAck.CmdId)

		if desc.phase == COMMIT || leaderFastAck.Ballot != r.ballot {
			return
		}

		desc.phase = COMMIT
		desc.deliver()
	} else {
		if r.status != FOLLOWER {
			return
		}

		desc := r.getCmdDesc(leaderFastAck.CmdId)

		if desc.phase == COMMIT || leaderFastAck.Ballot != r.ballot {
			return
		}

		for desc.phase != PAYLOAD_ONLY {
			desc.cond.Wait()
			if desc.phase == COMMIT || leaderFastAck.Ballot != r.ballot ||
				r.status != FOLLOWER {
				return
			}
		}

		desc.phase = COMMIT
		desc.dep = leaderFastAck.Dep
		desc.deliver()
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

func (r *Replica) leader() int32 {
	return leader(r.ballot, r.N)
}

func (r *Replica) getCmdDesc(cmdId CommandId) *commandDesc {
	WQ := r.qs.WQ(r.ballot)
	desc, exists := r.cmdDescs[cmdId]
	if !exists {
		desc = &commandDesc{}
		desc.cond = sync.NewCond(r)

		acceptFastAndSlowAck := func(msg interface{}) bool {
			if desc.fastAndSlowAcks.leaderMsg == nil {
				return true
			}
			switch leaderMsg := desc.fastAndSlowAcks.leaderMsg.(type) {
			case *MFastAck:
				return msg.(*MFastAck).Dep == nil ||
					(Dep(leaderMsg.Dep)).Equals(msg.(*MFastAck).Dep)
			case *MSlowAck:
				return WQ.contains(r.Id) ||
					(Dep(leaderMsg.Dep)).Equals(msg.(*MSlowAck).Dep)
			case *MLightSlowAck:
				return WQ.contains(r.Id)
			}

			return false
		}
		desc.fastAndSlowAcks =
			newMsgSet(WQ, acceptFastAndSlowAck, r.handleFastAndSlowAcks)

		desc.deliver = func() {
			if desc.phase != COMMIT || desc.delivered || !r.Exec {
				return
			}

			for _, cmdIdPrime := range desc.dep {
				descPrime := r.getCmdDesc(cmdIdPrime)
				for !descPrime.delivered {
					descPrime.cond.Wait()
					if desc.phase != COMMIT || desc.delivered || !r.Exec {
						return
					}
				}
			}

			desc.delivered = true
			desc.cond.Broadcast()

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
			r.ReplyProposeTS(proposeReply,
				desc.propose.Reply, desc.propose.Mutex)
		}

		desc.delivered = false

		r.cmdDescs[cmdId] = desc
	}

	return desc
}

func (r *Replica) generateDepOf(cmd state.Command, cmdId CommandId) Dep {
	info, exists := r.keysInfo[cmd.K]

	if exists {
		var cdep Dep

		if cmd.Op == state.GET {
			cdep = info.clientLastWrite
		} else {
			cdep = info.clientLastCmd
		}

		dep := make([]CommandId, len(cdep))
		copy(dep, cdep)

		return dep
	} else {
		return []CommandId{}
	}
}

func (r *Replica) addCmdInfo(cmd state.Command, cmdId CommandId) {
	info, exists := r.keysInfo[cmd.K]

	if !exists {
		info = &keyInfo{
			clientLastWrite: []CommandId{},
			clientLastCmd:   []CommandId{},
			lastWriteIndex:  make(map[int32]int),
			lastCmdIndex:    make(map[int32]int),
		}
		r.keysInfo[cmd.K] = info
	}

	info.add(cmd, cmdId)
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
