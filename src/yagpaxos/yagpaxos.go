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

	//quorum

	cmdDescs map[CommandId]*CommandDesc

	cs CommunicationSupply
}

type CommandDesc struct {
	sync.Mutex

	phase int
	cmd   state.Command
	//dep
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
	}

	return &r
}
