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
	info     map[state.Key]*keyInfo

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
		info:     make(map[state.Key]*keyInfo),

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

	return &r
}

func (r *Replica) generateDepOf(cmd state.Command, cmdId CommandId) Dep {
	info, exists := r.info[cmd.K]

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
	info, exists := r.info[cmd.K]

	if !exists {
		info = &keyInfo{
			clientLastWrite: []CommandId{},
			clientLastCmd:   []CommandId{},
			lastWriteIndex:  make(map[int32]int),
			lastCmdIndex:    make(map[int32]int),
		}
		r.info[cmd.K] = info
	}

	info.add(cmd, cmdId)
}
