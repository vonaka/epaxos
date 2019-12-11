package yagpaxos

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"state"
	//"sync"
	"time"

	"github.com/orcaman/concurrent-map"
)

type Replica struct {
	*genericsmr.Replica
	//sync.Mutex

	ballot  int32
	cballot int32
	status  int

	cmdDescs cmap.ConcurrentMap

	cs CommunicationSupply

	proposeProbe           *probe
	fastAckFromLeaderProbe *probe
	fastAckCCProbe         *probe
}

type commandDesc struct {
	//sync.Mutex

	phase   int
	cmd     state.Command
	dep     Dep
	propose *genericsmr.Propose

	fastAndSlowAcks *msgSet

	preAccOrPayloadOnlyCondF *condF
	payloadOnlyCondF         *condF

	msgs   chan interface{}
	active bool
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

var qs quorumSet

func NewReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply bool, failures int) *Replica {

	r := &Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  FOLLOWER,

		cmdDescs: cmap.New(),

		proposeProbe:           newProbe("propose"),
		fastAckFromLeaderProbe: newProbe("fa from leader"),
		fastAckCCProbe:         newProbe("fa cc"),

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
	}

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

	qs = newQuorumSet(r.N/2+1, r.N)

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
			desc, new := r.getCmdDesc(cmdId)
			if new {
				go func() {
					r.handlePropose(propose)
					for desc.active && !r.Shutdown {
						switch msg := (<-desc.msgs).(type) {
						case *genericsmr.Propose:
							r.handlePropose(msg)
						case *MFastAck:
							r.handleFastAck(msg)
						}
					}
				}()
			} else {
				go func() {
					desc.msgs <- propose
				}()
			}

			//go r.handlePropose(propose)

		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			desc, new := r.getCmdDesc(fastAck.CmdId)
			if new {
				go func() {
					r.handleFastAck(fastAck)
					for desc.active && !r.Shutdown {
						switch msg := (<-desc.msgs).(type) {
						case *genericsmr.Propose:
							r.handlePropose(msg)
						case *MFastAck:
							r.handleFastAck(msg)
						}
					}
				}()
			} else {
				go func() {
					desc.msgs <- fastAck
				}()
			}
			//fastAck := m.(*MFastAck)
			//go r.handleFastAck(fastAck)

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

		case m := <-r.cs.collectChan:
			collect := m.(*MCollect)
			go r.handleCollect(collect)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose) {
	/*r.Lock()
	r.proposeProbe.start()
	r.Unlock()
	defer func() {
		r.Lock()
		r.proposeProbe.stop()
		r.Unlock()
	}()*/

	WQ := r.WQ()
	cmdId := CommandId{
		ClientId: msg.ClientId,
		SeqNum:   msg.CommandId,
	}
	desc, _ := r.getCmdDesc(cmdId)

	//desc.Lock()

	if desc.propose != nil {
		//desc.Unlock()
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if !WQ.contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		desc.preAccOrPayloadOnlyCondF.recall()
		desc.payloadOnlyCondF.recall()
		//desc.Unlock()
		return
	}

	if r.Id == r.leader() {
		desc.phase = ACCEPT
	} else {
		desc.phase = PRE_ACCEPT
		desc.preAccOrPayloadOnlyCondF.recall()
	}

	dep := []CommandId{}
	desc.dep = dep
	//desc.Unlock()

	fastAck := &MFastAck{
		Replica: r.Id,
		Ballot:  r.ballot,
		CmdId:   cmdId,
		Dep:     dep,
	}

	go r.sendToAll(fastAck, r.cs.fastAckRPC)
	r.handleFastAck(fastAck)
}

func (r *Replica) handleFastAck(msg *MFastAck) {
	if msg.Replica == r.leader() && r.WQ().contains(r.Id) {
		r.fastAckFromLeaderToWQ(msg)
	} else {
		r.commonCaseFastAck(msg)
	}
}

func (r *Replica) fastAckFromLeaderToWQ(msg *MFastAck) {
	/*r.Lock()
	r.fastAckFromLeaderProbe.start()
	r.Unlock()
	defer func() {
		r.Lock()
		r.fastAckFromLeaderProbe.stop()
		r.Unlock()
	}()*/

	if (r.status != FOLLOWER && r.status != LEADER) || r.ballot != msg.Ballot {
		return
	}

	desc, _ := r.getCmdDesc(msg.CmdId)
	//desc.Lock()

	if r.status == LEADER {
		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
		//desc.Unlock()
		return
	}

	if desc.phase == ACCEPT || desc.phase == COMMIT {
		//desc.Unlock()
		return
	}


	desc.preAccOrPayloadOnlyCondF.call(func() {
		if r.status != FOLLOWER || r.ballot != msg.Ballot {
			return
		}

		// TODO: make sure that
		//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}

		desc.phase = ACCEPT

		desc.fastAndSlowAcks.add(msg.Replica, r.ballot, true, msg)
	})

	//desc.Unlock()
}

func (r *Replica) commonCaseFastAck(msg *MFastAck) {
	/*r.Lock()
	r.fastAckCCProbe.start()
	r.Unlock()
	defer func() {
		r.Lock()
		r.fastAckCCProbe.stop()
		r.Unlock()
	}()*/

	if (r.status != FOLLOWER && r.status != LEADER) || r.ballot != msg.Ballot {
		return
	}

	desc, _ := r.getCmdDesc(msg.CmdId)

	//desc.Lock()
	//defer desc.Unlock()

	if desc.phase == COMMIT {
		return
	}

	desc.fastAndSlowAcks.add(msg.Replica, r.ballot,
		msg.Replica == r.leader(), msg)
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

	if r.status != LEADER && r.status != FOLLOWER {
		return
	}

	WQ := r.WQ()
	if WQ.contains(r.Id) {
		desc, _ := r.getCmdDesc(leaderFastAck.CmdId)

		//desc.Lock()
		//defer desc.Unlock()

		if desc.phase == COMMIT || leaderFastAck.Ballot != r.ballot {
			return
		}

		desc.phase = COMMIT
		r.deliver(leaderFastAck.CmdId, desc)
	} else {
		if r.status != FOLLOWER {
			return
		}

		desc, _ := r.getCmdDesc(leaderFastAck.CmdId)

		//desc.Lock()
		//defer desc.Unlock()

		if desc.phase == COMMIT || leaderFastAck.Ballot != r.ballot {
			return
		}

		desc.payloadOnlyCondF.call(func() {
			if r.status != FOLLOWER || leaderFastAck.Ballot != r.ballot {
				return
			}

			desc.phase = COMMIT
			desc.dep = leaderFastAck.Dep
			r.deliver(leaderFastAck.CmdId, desc)
		})
	}
}

func (r *Replica) handleSlowAck(msg *MSlowAck) {

}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck) {

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

	desc.active = false

	if desc.cmd.Op == state.NONE {
		return
	}

	dlog.Printf("Executing " + desc.cmd.String())
	v := desc.cmd.Execute(r.State)

	if !r.Dreply {
		return
	}

	go func() {
		proposeReply := &genericsmrproto.ProposeReplyTS{
			OK:        genericsmr.TRUE,
			CommandId: desc.propose.CommandId,
			Value:     v,
			Timestamp: desc.propose.Timestamp,
		}
		r.ReplyProposeTS(proposeReply,
			desc.propose.Reply, desc.propose.Mutex)

		r.cmdDescs.Remove(cmdId.String())
	}()
}

func (r *Replica) leader() int32 {
	return leader(r.ballot, r.N)
}

func (r *Replica) WQ() quorum {
	return qs.WQ(r.ballot)
}

func (r *Replica) getCmdDesc(cmdId CommandId) (*commandDesc, bool) {
	new := false
	res := r.cmdDescs.Upsert(cmdId.String(), nil,
		func(exists bool, mapV interface{}, _ interface{}) interface{} {
			if exists {
				return mapV
			}

			desc := &commandDesc{
				msgs:   make(chan interface{}, 3),
				active: true,
			}

			desc.preAccOrPayloadOnlyCondF = newCondF(func() bool {
				return desc.phase == PRE_ACCEPT || desc.phase == PAYLOAD_ONLY
			})

			desc.payloadOnlyCondF = newCondF(func() bool {
				return desc.phase == PAYLOAD_ONLY
			})

			acceptFastAndSlowAck := func(msg interface{}) bool {
				if msg == nil || desc.fastAndSlowAcks.leaderMsg == nil {
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

			new = true
			return desc
		})

	return res.(*commandDesc), new

	/*element, exists := r.cmdDescs.Get(cmdId.String())
	if exists {
		return element.(*commandDesc), false
	}

	desc := &commandDesc{
		msgs: make(chan interface{}, 3),
		active: true,
	}

	desc.preAccOrPayloadOnlyCondF = newCondF(func() bool {
		return desc.phase == PRE_ACCEPT || desc.phase == PAYLOAD_ONLY
	})

	desc.payloadOnlyCondF = newCondF(func() bool {
		return desc.phase == PAYLOAD_ONLY
	})

	acceptFastAndSlowAck := func(msg interface{}) bool {
		if msg == nil || desc.fastAndSlowAcks.leaderMsg == nil {
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

	if ok := r.cmdDescs.SetIfAbsent(cmdId.String(), desc); ok {
		return desc, true
	}

	// FIXME: what if gc removed desc here?
	element, _ = r.cmdDescs.Get(cmdId.String())
	return element.(*commandDesc), false*/
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
