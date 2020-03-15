package yagpaxos

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"log"
	"state"
	"strconv"
	"sync"
	"time"

	"github.com/orcaman/concurrent-map"
)

type PaxosReplica struct {
	*genericsmr.Replica

	ballot  int32
	cballot int32
	status  int

	lastCmdSlot int
	cmdDescs    cmap.ConcurrentMap
	delivered   cmap.ConcurrentMap
	history     []paxosCommandStaticDesc

	qs quorumSet
	cs paxosCommunicationSupply

	descPool sync.Pool
}

type paxosCommandDesc struct {
	phase   int
	cmd     state.Command
	cmdSlot int
	propose *genericsmr.Propose
	twoBs   *msgSet

	msgs   chan interface{}
	active bool
}

type paxosCommandStaticDesc struct {
	cmdSlot int
	phase   int
	cmd     state.Command
}

type paxosCommunicationSupply struct {
	maxLatency time.Duration

	oneAChan chan fastrpc.Serializable
	oneBChan chan fastrpc.Serializable
	twoAChan chan fastrpc.Serializable
	twoBChan chan fastrpc.Serializable
	syncChan chan fastrpc.Serializable

	oneARPC uint8
	oneBRPC uint8
	twoARPC uint8
	twoBRPC uint8
	syncRPC uint8
}

func NewPaxosReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply bool, failures int) *PaxosReplica {

	r := &PaxosReplica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		lastCmdSlot: 0,
		cmdDescs:    cmap.New(),
		delivered:   cmap.New(),
		history:     make([]paxosCommandStaticDesc, HISTORY_SIZE),

		cs: paxosCommunicationSupply{
			maxLatency: 0,

			oneAChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			oneBChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			twoAChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			twoBChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			syncChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
		},

		descPool: sync.Pool{
			New: func() interface{} {
				return &paxosCommandDesc{}
			},
		},
	}

	r.qs = newQuorumSet(r.N/2+1, r.N)

	r.cs.oneARPC = r.RegisterRPC(new(M1A), r.cs.oneAChan)
	r.cs.oneBRPC = r.RegisterRPC(new(M1B), r.cs.oneBChan)
	r.cs.twoARPC = r.RegisterRPC(new(M2A), r.cs.twoAChan)
	r.cs.twoBRPC = r.RegisterRPC(new(M2B), r.cs.twoBChan)
	r.cs.syncRPC = r.RegisterRPC(new(MPaxosSync), r.cs.twoBChan)

	go r.run()

	return r
}

func (r *PaxosReplica) run() {
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
			desc := r.getCmdDesc(r.lastCmdSlot, propose)
			if desc == nil {
				log.Fatal("Got propose for the delivered command:",
					propose.ClientId, propose.CommandId)
			}
			r.lastCmdSlot++
		}
	}
}

func (r *PaxosReplica) handlePropose(msg *genericsmr.Propose,
	desc *paxosCommandDesc, slot int) {

	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg
	desc.cmd = msg.Command

	if r.Id != r.leader() {
		return
	}

	desc.cmdSlot = slot

	twoA := &M2A{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cmd:     msg.Command,
		CmdSlot: slot,
	}

	go r.sendToAll(twoA, r.cs.twoARPC)
	r.handle2A(twoA, desc)
}

func (r *PaxosReplica) handle2A(msg *M2A, desc *paxosCommandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.cmd = msg.Cmd
	desc.cmdSlot = msg.CmdSlot

	if !r.AQ().contains(r.Id) {
		return
	}

	twoB := &M2B{
		Replica: r.Id,
		Ballot:  msg.Ballot,
		CmdSlot: msg.CmdSlot,
	}

	go r.sendToAll(twoB, r.cs.twoBRPC)
	r.handle2B(twoB, desc)
}

func (r *PaxosReplica) handle2B(msg *M2B, desc *paxosCommandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.twoBs.add(msg.Replica, false, msg)
}

func (r *PaxosReplica) deliver(slot int, desc *paxosCommandDesc) {
	if r.delivered.Has(strconv.Itoa(slot)) || desc.phase != COMMIT || !r.Exec {
		return
	}

	if !r.delivered.Has(strconv.Itoa(slot - 1)) {
		return
	}

	r.delivered.Set(strconv.Itoa(slot), struct{}{})
	r.getCmdDesc(slot+1, "deliver")
	r.history[slot].cmdSlot = slot
	r.history[slot].phase = desc.phase
	r.history[slot].cmd = desc.cmd
	desc.active = false
	r.cmdDescs.Remove(strconv.Itoa(slot))
	defer r.descPool.Put(desc)

	dlog.Printf("Executing " + desc.cmd.String())
	v := desc.cmd.Execute(r.State)

	if desc.propose == nil || !r.Dreply {
		return
	}

	rep := &genericsmrproto.ProposeReplyTS{
		OK:        genericsmr.TRUE,
		CommandId: desc.propose.CommandId,
		Value:     v,
		Timestamp: desc.propose.Timestamp,
	}
	r.ReplyProposeTS(rep, desc.propose.Reply, desc.propose.Mutex)
}

func (r *PaxosReplica) handleDesc(desc *paxosCommandDesc, slot int) {
	for desc.active {
		switch msg := (<-desc.msgs).(type) {

		case *genericsmr.Propose:
			r.handlePropose(msg, desc, slot)

		case *M2A:
			r.handle2A(msg, desc)

		case *M2B:
			r.handle2B(msg, desc)

		case string:
			if msg == "deliver" {
				r.deliver(slot, desc)
			}
		}
	}
}

func (r *PaxosReplica) leader() int32 {
	return leader(r.ballot, r.N)
}

func (r *PaxosReplica) AQ() quorum {
	return r.qs.AQ(r.ballot)
}

func (r *PaxosReplica) getCmdDesc(slot int, msg interface{}) *paxosCommandDesc {

	if r.delivered.Has(strconv.Itoa(slot)) {
		return nil
	}

	res := r.cmdDescs.Upsert(strconv.Itoa(slot), nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				desc := mapV.(*paxosCommandDesc)
				if msg != nil {
					desc.msgs <- msg
				}

				return desc
			}

			desc := r.descPool.Get().(*paxosCommandDesc)
			desc.cmdSlot = -1
			desc.msgs = make(chan interface{}, 8)
			desc.active = true
			desc.phase = START
			desc.propose = nil

			desc.twoBs = newMsgSet(r.AQ(), func(msg interface{}) bool {
				return true
			}, get2BsHandler(r, desc))

			go r.handleDesc(desc, slot)

			if msg != nil {
				desc.msgs <- msg
			}

			return desc
		})

	return res.(*paxosCommandDesc)
}

func get2BsHandler(r *PaxosReplica, desc *paxosCommandDesc) msgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {
		desc.phase = COMMIT
		r.deliver(msgs[0].(*M2B).CmdSlot, desc)
	}
}

func (r *PaxosReplica) sendToAll(msg fastrpc.Serializable, rpc uint8) {
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
