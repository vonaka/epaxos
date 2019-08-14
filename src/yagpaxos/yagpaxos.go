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

}

func (r *Replica) handleFastAck(msg *MFastAck) {

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
