package yagpaxos

import (
	"fastrpc"
	"genericsmr"
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

	cmdDescs map[CommandId]*commandDesc
	keysInfo map[state.Key]*keyInfo

	cs CommunicationSupply
}

type commandDesc struct {
	sync.Mutex

	phase   int
	cmd     state.Command
	dep     Dep
	propose *genericsmr.Propose

	proposeCond     *sync.Cond
	fastAndSlowAcks *msgSet
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
	newLeaderChan    chan fastrpc.Serializable
	newLeaderAckChan chan fastrpc.Serializable
	syncChan         chan fastrpc.Serializable
	syncAckChan      chan fastrpc.Serializable
	flushChan        chan fastrpc.Serializable

	fastAckRPC      uint8
	slowAckRPC      uint8
	newLeaderRPC    uint8
	newLeaderAckRPC uint8
	syncRPC         uint8
	syncAckRPC      uint8
	flushRPC        uint8
}

func NewReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply bool) *Replica {

	r := Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply),

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

	if leader(r.ballot, r.N) == r.Id {
		r.status = LEADER
	}

	r.qs = newQuorumSet(r.N/2+1, r.N)

	r.cs.fastAckRPC =
		r.RegisterRPC(new(MFastAck), r.cs.fastAckChan)
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

	desc, exists := r.cmdDescs[cmdId]
	if !exists {
		desc = &commandDesc{}
		desc.proposeCond = sync.NewCond(r)
		r.cmdDescs[cmdId] = desc
	}

	if desc.propose != nil {
		r.Unlock()
		return
	}

	desc.propose = msg
	desc.proposeCond.Broadcast()

	desc.cmd = msg.Command

	acceptFastAndSlowAck := func(msg interface{}) bool {
		if desc.fastAndSlowAcks.leaderMsg == nil {
			return true
		}
		switch leaderMsg := desc.fastAndSlowAcks.leaderMsg.(type) {
		case *MFastAck:
			return (Dep(leaderMsg.Dep)).Equals(msg.(*MFastAck).Dep)
		case *MSlowAck:
			return (Dep(leaderMsg.Dep)).Equals(msg.(*MSlowAck).Dep)
		}

		return false
	}
	desc.fastAndSlowAcks =
		newMsgSet(WQ, acceptFastAndSlowAck, r.handleFastAndSlowAcks)

	if !WQ.contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		r.Unlock()
		return
	}

	if r.Id == leader(r.ballot, r.N) {
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

}

func (r *Replica) handleFastAndSlowAcks(leaderMsg interface{},
	msgs []interface{}) {

}

func (r *Replica) handleSlowAck(msg *MSlowAck) {

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
