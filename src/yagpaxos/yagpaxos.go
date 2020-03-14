package yagpaxos

import (
	"dlog"
	"fastrpc"
	"fmt"
	"genericsmr"
	"log"
	"state"
	"sync"
	"time"

	"github.com/orcaman/concurrent-map"
)

const HISTORY_SIZE = 1001001

type Replica struct {
	*genericsmr.Replica

	ballot  int32
	cballot int32
	status  int

	cmdDescs  cmap.ConcurrentMap
	delivered cmap.ConcurrentMap

	repchan *replyChan
	history []commandStaticDesc
	keys    map[state.Key]keyInfo
	keysL   sync.Mutex

	qs quorumSet
	cs CommunicationSupply

	descPool    sync.Pool
}

type commandDesc struct {
	phase   int
	cmd     state.Command
	dep     Dep
	propose *genericsmr.Propose

	fastAndSlowAcks *msgSet
	afterPropagate  *condF

	msgs     chan interface{}
	active   bool
	slowPath bool

	successors  []*commandDesc
	successorsL sync.Mutex

	// will be executed before sending
	// NewLeaderAck message
	defered func()
}

type commandStaticDesc struct {
	cmdId    CommandId
	phase    int
	cmd      state.Command
	dep      Dep
	slowPath bool
	defered  func()
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
	thrifty, exec, lread, dreply bool, failures int) *Replica {

	r := &Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		cmdDescs:  cmap.New(),
		delivered: cmap.New(),
		history:   make([]commandStaticDesc, HISTORY_SIZE),
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

		descPool: sync.Pool{
			New: func() interface{} {
				return &commandDesc{}
			},
		},
	}

	r.repchan = NewReplyChan(r)
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
	r.cs.collectRPC =
		r.RegisterRPC(new(MCollect), r.cs.collectChan)

	hookUser1(func() {
		totalNum := 0
		slowPaths := 0
		for i := 0; i < HISTORY_SIZE; i++ {
			if r.history[i].dep == nil {
				continue
			}
			totalNum++
			if r.history[i].slowPath {
				slowPaths++
			}
		}

		fmt.Printf("Total number of commands: %d\n", totalNum)
		fmt.Printf("Number of slow paths: %d\n", slowPaths)
	})

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

	var cmdId CommandId

	for !r.Shutdown {
		select {
		case propose := <-r.ProposeChan:
			cmdId.ClientId = propose.ClientId
			cmdId.SeqNum = propose.CommandId
			desc := r.getCmdDesc(cmdId, propose)
			if desc == nil {
				log.Fatal("Got propose for the delivered command:", cmdId)
			}

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

func (r *Replica) handlePropose(msg *genericsmr.Propose,
	desc *commandDesc, cmdId CommandId) {

	if r.status != NORMAL || desc.phase != START || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if !r.AQ().contains(r.Id) {
		desc.phase = PAYLOAD_ONLY
		desc.afterPropagate.recall()
		return
	}

	desc.dep = r.getDepAndUpdateInfo(msg.Command, cmdId)
	desc.phase = PRE_ACCEPT
	if desc.afterPropagate.recall() && desc.slowPath {
		// in this case a process already sent a MSlowAck
		// message, hence, no need to sent MFastAck
		return
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
	if msg.Replica == r.leader() {
		r.fastAckFromLeader(msg, desc)
	} else {
		r.commonCaseFastAck(msg, desc)
	}
}

func (r *Replica) fastAckFromLeader(msg *MFastAck, desc *commandDesc) {
	if !r.AQ().contains(r.Id) {
		desc.afterPropagate.call(func() {
			if r.status == NORMAL && r.ballot == msg.Ballot {
				desc.dep = msg.Dep
			}
			desc.fastAndSlowAcks.add(msg.Replica, true, msg)
		})
		return
	}

	desc.afterPropagate.call(func() {
		if r.status != NORMAL || r.ballot != msg.Ballot {
			return
		}

		// TODO: make sure that
		//    ∀ id' ∈ d. phase[id'] ∈ {ACCEPT, COMMIT}
		//
		// seems to be satisfied already

		desc.phase = ACCEPT
		desc.fastAndSlowAcks.add(msg.Replica, true, msg)
		dep := Dep(msg.Dep)
		equals, diffs := desc.dep.EqualsAndDiff(dep)

		if !equals {
			oldDefered := desc.defered
			desc.defered = func() {
				for cmdId := range diffs {
					if r.delivered.Has(cmdId.String()) {
						continue
					}
					descPrime := r.getCmdDesc(cmdId, nil)
					if descPrime.phase == PRE_ACCEPT {
						descPrime.phase = PAYLOAD_ONLY
					}
				}
				oldDefered()
			}

			desc.dep = dep
			desc.slowPath = true

			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
				Ballot:  r.ballot,
				CmdId:   msg.CmdId,
			}

			go r.sendToAll(lightSlowAck, r.cs.lightSlowAckRPC)
			r.handleLightSlowAck(lightSlowAck, desc)
		}
	})
}

func (r *Replica) commonCaseFastAck(msg *MFastAck, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.fastAndSlowAcks.add(msg.Replica, false, msg)
}

func getFastAndSlowAcksHandler(r *Replica, desc *commandDesc) msgSetHandler {
	return func (leaderMsg interface{}, msgs []interface{}) {

		if leaderMsg == nil {
			return
		}

		leaderFastAck := leaderMsg.(*MFastAck)

		desc.phase = COMMIT

		for _, depCmdId := range desc.dep {
			depDesc := r.getCmdDesc(depCmdId, nil)
			if depDesc == nil {
				continue
			}
			depDesc.successorsL.Lock()
			depDesc.successors = append(depDesc.successors, desc)
			depDesc.successorsL.Unlock()
		}

		r.deliver(leaderFastAck.CmdId, desc)
	}
}

func (r *Replica) handleSlowAck(msg *MSlowAck, desc *commandDesc) {
	r.commonCaseFastAck((*MFastAck)(msg), desc)
}

func (r *Replica) handleLightSlowAck(msg *MLightSlowAck, desc *commandDesc) {
	r.commonCaseFastAck(&MFastAck{
		Replica: msg.Replica,
		Ballot:  msg.Ballot,
		CmdId:   msg.CmdId,
		Dep:     nil,
	}, desc)
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
	// TODO: what if desc.propose is nil ?
	//       is that possible ?

	if r.delivered.Has(cmdId.String()) || desc.phase != COMMIT || !r.Exec {
		return
	}

	for _, cmdIdPrime := range desc.dep {
		if !r.delivered.Has(cmdIdPrime.String()) {
			return
		}
	}

	r.delivered.Set(cmdId.String(), struct{}{})

	if isNoop(desc.cmd) {
		return
	}

	dlog.Printf("Executing " + desc.cmd.String())
	v := desc.cmd.Execute(r.State)

	desc.successorsL.Lock()
	if desc.successors != nil {
		for i := 0; i < len(desc.successors); i++ {
			go func(msgs chan interface{}) {
				msgs <- "deliver"
			}(desc.successors[i].msgs)
		}
	}
	desc.successorsL.Unlock()

	if !r.Dreply {
		return
	}

	r.repchan.reply(v, desc, cmdId)
}

func (r *Replica) leader() int32 {
	return leader(r.ballot, r.N)
}

func (r *Replica) AQ() quorum {
	return r.qs.AQ(r.ballot)
}

func (r *Replica) handleDesc(desc *commandDesc, cmdId CommandId) {
	for desc.active {
		switch msg := (<-desc.msgs).(type) {

		case *genericsmr.Propose:
			r.handlePropose(msg, desc, cmdId)

		case *MFastAck:
			r.handleFastAck(msg, desc)

		case *MSlowAck:
			r.handleSlowAck(msg, desc)

		case *MLightSlowAck:
			r.handleLightSlowAck(msg, desc)

		case string:
			if msg == "deliver" {
				r.deliver(cmdId, desc)
			}

		case int:
			r.history[msg].cmdId = cmdId
			r.history[msg].phase = desc.phase
			r.history[msg].cmd = desc.cmd
			r.history[msg].dep = desc.dep
			r.history[msg].slowPath = desc.slowPath
			r.history[msg].defered = desc.defered
			desc.active = false
			r.cmdDescs.Remove(cmdId.String())
			r.descPool.Put(desc)
			return
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
				desc := mapV.(*commandDesc)
				if msg != nil {
					desc.msgs <- msg
				}

				return desc
			}

			desc := r.descPool.Get().(*commandDesc)
			desc.dep = nil
			desc.msgs = make(chan interface{}, 8)
			desc.active = true
			desc.phase = START
			desc.successors = nil
			desc.slowPath = false
			desc.defered = func() {}
			desc.propose = nil

			desc.afterPropagate = newCondF(func() bool {
				return desc.propose != nil
			})

			acceptFastAndSlowAck := func(msg interface{}) bool {
				if desc.fastAndSlowAcks.leaderMsg == nil {
					return true
				}
				leaderFastAck := desc.fastAndSlowAcks.leaderMsg.(*MFastAck)
				fastAck := msg.(*MFastAck)
				return fastAck.Dep == nil ||
					(Dep(leaderFastAck.Dep)).Equals(fastAck.Dep)
			}

			desc.fastAndSlowAcks = newMsgSet(r.AQ(), acceptFastAndSlowAck,
				getFastAndSlowAcksHandler(r, desc))

			go r.handleDesc(desc, cmdId)

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
			r.keys[key] = info
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
