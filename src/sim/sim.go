package sim

import (
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"log"
	"math/rand"
	"state"
	"time"
	y "yagpaxos"
)

type Replica struct {
	*genericsmr.Replica

	ballot   int32
	conflict int
	cmdDescs map[y.CommandId]*commandDescSim

	sender  y.Sender

	AQ y.Quorum
	qs y.QuorumSet
	cs CommunicationSupply
}

type commandDescSim struct {
	propose   *genericsmr.Propose
	slowPath  bool
	dep       y.Dep
	delivered bool

	fastAndSlowAcks *y.MsgSet
	afterPropagate  *y.OptCondF
}

type CommunicationSupply struct {
	maxLatency time.Duration

	fastAckChan      chan fastrpc.Serializable
	slowAckChan      chan fastrpc.Serializable
	lightSlowAckChan chan fastrpc.Serializable

	fastAckRPC      uint8
	slowAckRPC      uint8
	lightSlowAckRPC uint8
}

var (
	goodDep = y.Dep{y.CommandId{
		ClientId: 24,
		SeqNum:   24,
	}}
	badDep  = y.Dep{y.CommandId{
		ClientId: 42,
		SeqNum:   42,
	}}
)

func NewReplica(id int, addrs []string, f int, qfile string, conflict int) *Replica {

	rand.Seed(time.Now().UnixNano())

	r := &Replica{
		Replica: genericsmr.NewReplica(id, addrs, false, false, false, false, f),

		ballot:   0,
		conflict: conflict,
		cmdDescs: make(map[y.CommandId]*commandDescSim),

		cs: CommunicationSupply{
			maxLatency: 0,

			fastAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			slowAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
			lightSlowAckChan: make(chan fastrpc.Serializable,
				genericsmr.CHAN_BUFFER_SIZE),
		},
	}

	r.sender = y.NewSender(r.Replica)
	r.qs = y.NewQuorumSet(r.N/2+1, r.N)

	AQ, leaderId, err := y.NewQuorumFromFile(qfile, r.Replica)
	if err == nil {
		r.AQ = AQ
		r.ballot = leaderId
	} else if err == y.NO_QUORUM_FILE {
		r.AQ = r.qs.AQ(r.ballot)
	} else {
		log.Fatal(err)
	}

	r.cs.fastAckRPC =
		r.RegisterRPC(new(y.MFastAck), r.cs.fastAckChan)
	r.cs.slowAckRPC =
		r.RegisterRPC(new(y.MSlowAck), r.cs.slowAckChan)
	r.cs.lightSlowAckRPC =
		r.RegisterRPC(new(y.MLightSlowAck), r.cs.lightSlowAckChan)

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
			r.handlePropose(propose, r.getCmdDesc(y.CommandId{
				ClientId: propose.ClientId,
				SeqNum:   propose.CommandId,
			}))
		case m := <-r.cs.fastAckChan:
			fastAck := m.(*y.MFastAck)
			r.handleFastAck(fastAck, r.getCmdDesc(fastAck.CmdId))
		case m := <-r.cs.slowAckChan:
			slowAck := m.(*y.MSlowAck)
			r.handleSlowAck(slowAck, r.getCmdDesc(slowAck.CmdId))
		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*y.MLightSlowAck)
			r.handleLightSlowAck(lightSlowAck, r.getCmdDesc(lightSlowAck.CmdId))
		}
	}
}

func (r *Replica) handlePropose(p *genericsmr.Propose, desc *commandDescSim) {
	desc.propose = p
	if !r.AQ.Contains(r.Id) {
		desc.afterPropagate.Recall()
		return
	}
	if desc.afterPropagate.Recall() && desc.slowPath {
		return
	}
	dep := func() y.Dep {
		if r.conflict == 0 || r.Id == leader(r.ballot, r.N) {
			return goodDep
		} else if r.conflict >= 100 {
			return badDep
		}
		if rand.Intn(100) > r.conflict {
			return goodDep
		}
		return badDep
	}()
	desc.dep = dep
	fastAck := &y.MFastAck{
		CmdId: y.CommandId{
			ClientId: p.ClientId,
			SeqNum:   p.CommandId,
		},
		Replica: r.Id,
		Dep:     dep,
	}
	r.sender.SendToAll(fastAck, r.cs.fastAckRPC)
	r.handleFastAck(fastAck, desc)
}

func (r *Replica) handleFastAck(fastAck *y.MFastAck, desc *commandDescSim) {
	if fastAck.Replica != leader(r.ballot, r.N) {
		r.commonCaseFastAck(fastAck, desc)
		return
	}
	if !r.AQ.Contains(r.Id) {
		desc.afterPropagate.Call(func() {
			desc.fastAndSlowAcks.Add(fastAck.Replica, true, fastAck)
		})
		return
	}

	desc.afterPropagate.Call(func() {
		if !desc.dep.Equals(fastAck.Dep) {
			desc.fastAndSlowAcks.Add(fastAck.Replica, true, fastAck)
			desc.dep = fastAck.Dep
			desc.slowPath = true
			lightSlowAck := &y.MLightSlowAck{
				CmdId:   fastAck.CmdId,
				Replica: r.Id,
			}
			r.sender.SendToAll(lightSlowAck, r.cs.lightSlowAckRPC)
			r.handleLightSlowAck(lightSlowAck, desc)
		} else {
			desc.fastAndSlowAcks.Add(fastAck.Replica, true, fastAck)
		}
	})
}

func (r *Replica) commonCaseFastAck(fastAck *y.MFastAck, desc *commandDescSim) {
	desc.fastAndSlowAcks.Add(fastAck.Replica, false, fastAck)
}

func (r *Replica) handleSlowAck(slowAck *y.MSlowAck, desc *commandDescSim) {
	r.commonCaseFastAck((*y.MFastAck)(slowAck), desc)
}

func (r *Replica) handleLightSlowAck(lightSlowAck *y.MLightSlowAck, desc *commandDescSim) {
	r.commonCaseFastAck(&y.MFastAck{
		CmdId:   lightSlowAck.CmdId,
		Replica: lightSlowAck.Replica,
		Dep:     nil,
	}, desc)
}

func (r *Replica) getCmdDesc(cmdId y.CommandId) *commandDescSim {
	desc, exists := r.cmdDescs[cmdId]
	if exists {
		return desc
	}
	desc = &commandDescSim{
		propose:   nil,
		slowPath:  false,
		delivered: false,
	}

	acc := func(msg, leaderMsg interface{}) bool {
		if leaderMsg == nil {
			return true
		}
		return msg.(*y.MFastAck).Dep == nil || goodDep.Equals(msg.(*y.MFastAck).Dep)
	}

	free := func(_ interface{}) {}

	fastAndSlowHandler := func(leaderMsg interface{}, msgs []interface{}) {
		if leaderMsg == nil {
			return
		}
		dCmdId := leaderMsg.(*y.MFastAck).CmdId
		dDesc := r.getCmdDesc(dCmdId)
		if !dDesc.delivered && dDesc.propose.Collocated {
			dDesc.delivered = true
			rep := &genericsmrproto.ProposeReplyTS{
				OK:        genericsmr.TRUE,
				CommandId: dDesc.propose.CommandId,
				Value:     state.NIL(),
				Timestamp: dDesc.propose.Timestamp,
			}
			r.ReplyProposeTS(rep, dDesc.propose.Reply, dDesc.propose.Mutex)
		}
	}

	desc.fastAndSlowAcks = y.NewMsgSet(r.AQ, acc, free, fastAndSlowHandler)
	desc.afterPropagate = y.NewOptCondF(func() bool {
		return desc.propose != nil
	})

	r.cmdDescs[cmdId] = desc
	return desc
}

func leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}
