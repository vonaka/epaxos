package optpaxos

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
	"yagpaxos"

	"github.com/orcaman/concurrent-map"
)

const HISTORY_SIZE = 1001001

type Replica struct {
	*genericsmr.Replica

	ballot  int32
	cballot int32
	status  int

	isLeader bool

	lastCmdSlot int
	cmdDescs    cmap.ConcurrentMap
	delivered   cmap.ConcurrentMap
	proposes    cmap.ConcurrentMap
	history     []commandStaticDesc

	qs yagpaxos.QuorumSet
	cs communicationSupply

	descPool sync.Pool
}

type commandDesc struct {
	cmdId CommandId

	phase   int
	cmd     state.Command
	cmdSlot int
	propose *genericsmr.Propose
	twoBs   *yagpaxos.MsgSet

	msgs   chan interface{}
	active bool
}

type commandStaticDesc struct {
	cmdSlot int
	phase   int
	cmd     state.Command
}

type communicationSupply struct {
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

func NewReplica(replicaId int, peerAddrs []string,
	thrifty, exec, lread, dreply bool, failures int) *Replica {

	r := &Replica{
		Replica: genericsmr.NewReplica(replicaId, peerAddrs,
			thrifty, exec, lread, dreply, failures),

		ballot:  0,
		cballot: 0,
		status:  NORMAL,

		isLeader: false,

		lastCmdSlot: 0,
		cmdDescs:    cmap.New(),
		delivered:   cmap.New(),
		proposes:    cmap.New(),
		history:     make([]commandStaticDesc, HISTORY_SIZE),

		cs: communicationSupply{
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
				return &commandDesc{}
			},
		},
	}

	r.qs = yagpaxos.NewQuorumSet(r.N/2+1, r.N)

	r.cs.oneARPC = r.RegisterRPC(new(M1A), r.cs.oneAChan)
	r.cs.oneBRPC = r.RegisterRPC(new(M1B), r.cs.oneBChan)
	r.cs.twoARPC = r.RegisterRPC(new(M2A), r.cs.twoAChan)
	r.cs.twoBRPC = r.RegisterRPC(new(M2B), r.cs.twoBChan)
	r.cs.syncRPC = r.RegisterRPC(new(MPaxosSync), r.cs.twoBChan)

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
			if r.isLeader {
				desc := r.getCmdDesc(r.lastCmdSlot, propose)
				if desc == nil {
					log.Fatal("Got propose for the delivered command:",
						propose.ClientId, propose.CommandId)
				}
				r.lastCmdSlot++
			} else {
				cmdId.ClientId = propose.ClientId
				cmdId.SeqNum = propose.CommandId
				r.proposes.Set(cmdId.String(), propose)
			}

		case m :=<-r.cs.twoAChan:
			twoA := m.(*M2A)
			r.getCmdDesc(twoA.CmdSlot, twoA)

		case m :=<-r.cs.twoBChan:
			twoB := m.(*M2B)
			r.getCmdDesc(twoB.CmdSlot, twoB)
		}
	}
}

func (r *Replica) handlePropose(msg *genericsmr.Propose,
	desc *commandDesc, slot int) {

	if r.status != NORMAL || desc.propose != nil {
		return
	}

	desc.propose = msg

	twoA := &M2A{
		Replica: r.Id,
		Ballot:  r.ballot,
		Cmd:     msg.Command,
		CmdId:   CommandId{
			ClientId: msg.ClientId,
			SeqNum:   msg.CommandId,
		},
		CmdSlot: slot,
	}

	go r.sendToAll(twoA, r.cs.twoARPC)
	r.handle2A(twoA, desc)
}

func (r *Replica) handle2A(msg *M2A, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.cmd = msg.Cmd
	desc.cmdId = msg.CmdId
	desc.cmdSlot = msg.CmdSlot

	if !r.isLeader {
		defer func() {
			// deliver a command which waits for
			// a payload
			desc.msgs <- "deliver"
		}()
	}

	if !r.AQ().Contains(r.Id) {
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

func (r *Replica) handle2B(msg *M2B, desc *commandDesc) {
	if r.status != NORMAL || r.ballot != msg.Ballot {
		return
	}

	desc.twoBs.Add(msg.Replica, false, msg)
}

func (r *Replica) deliver(slot int, desc *commandDesc) {
	if r.delivered.Has(strconv.Itoa(slot)) || desc.phase != COMMIT || !r.Exec {
		return
	}

	if desc.cmdId.SeqNum == -1 {
		return
	}

	if slot > 0 && !r.delivered.Has(strconv.Itoa(slot - 1)) {
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

	p, exists := r.proposes.Get(desc.cmdId.String())
	if exists {
		desc.propose = p.(*genericsmr.Propose)
	}

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

func (r *Replica) handleDesc(desc *commandDesc, slot int) {
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

func (r *Replica) AQ() yagpaxos.Quorum {
	return r.qs.AQ(r.ballot)
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs,
	reply *genericsmrproto.BeTheLeaderReply) error {
	r.isLeader = true
	return nil
}

func (r *Replica) getCmdDesc(slot int, msg interface{}) *commandDesc {

	if r.delivered.Has(strconv.Itoa(slot)) {
		return nil
	}

	res := r.cmdDescs.Upsert(strconv.Itoa(slot), nil,
		func(exists bool, mapV, _ interface{}) interface{} {
			if exists {
				desc := mapV.(*commandDesc)
				if msg != nil {
					desc.msgs <- msg
				}

				return desc
			}

			desc := r.descPool.Get().(*commandDesc)
			desc.cmdSlot = -1
			desc.msgs = make(chan interface{}, 8)
			desc.active = true
			desc.phase = START
			desc.propose = nil
			desc.cmdId.SeqNum = -1

			desc.twoBs = yagpaxos.NewMsgSet(r.AQ(), func(msg interface{}) bool {
				return true
			}, get2BsHandler(r, desc))

			go r.handleDesc(desc, slot)

			if msg != nil {
				desc.msgs <- msg
			}

			return desc
		})

	return res.(*commandDesc)
}

func get2BsHandler(r *Replica, desc *commandDesc) yagpaxos.MsgSetHandler {
	return func(leaderMsg interface{}, msgs []interface{}) {
		desc.phase = COMMIT
		r.deliver(msgs[0].(*M2B).CmdSlot, desc)
	}
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
