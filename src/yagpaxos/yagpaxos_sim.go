package yagpaxos

import (
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"log"
	"math/rand"
	"state"
	"time"
)

type ReplicaSim struct {
	*genericsmr.Replica

	ballot   int32
	conflict int
	cmdDescs map[CommandId]*commandDescSim

	sender  Sender

	AQ Quorum
	qs QuorumSet
	cs CommunicationSupply
}

type commandDescSim struct {
	propose  *genericsmr.Propose
	slowPath bool
	dep      Dep

	fastAndSlowAcks *MsgSet
	afterPropagate  *OptCondF
}

var (
	goodDep = Dep{}
	badDep  = Dep{CommandId{
		ClientId: 42,
		SeqNum:   42,
	}}
)

func NewReplicaSim(id int, addrs []string, f int, qfile string, conflict int) *ReplicaSim {

	rand.Seed(time.Now().UnixNano())

	r := &ReplicaSim{
		Replica: genericsmr.NewReplica(id, addrs, false, false, false, false, f),

		ballot:   0,
		conflict: conflict,
		cmdDescs: make(map[CommandId]*commandDescSim),

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

	r.sender = NewSender(r.Replica)
	r.qs = NewQuorumSet(r.N/2+1, r.N)

	AQ, leaderId, err := NewQuorumFromFile(qfile, r.Replica)
	if err == nil {
		r.AQ = AQ
		r.ballot = leaderId
	} else if err == NO_QUORUM_FILE {
		r.AQ = r.qs.AQ(r.ballot)
	} else {
		log.Fatal(err)
	}

	r.cs.fastAckRPC =
		r.RegisterRPC(new(MFastAck), r.cs.fastAckChan)
	r.cs.slowAckRPC =
		r.RegisterRPC(new(MSlowAck), r.cs.slowAckChan)
	r.cs.lightSlowAckRPC =
		r.RegisterRPC(new(MLightSlowAck), r.cs.lightSlowAckChan)

	go r.run()

	return r
}

func (r *ReplicaSim) run() {
	r.ConnectToPeers()

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
			r.handlePropose(propose, r.getCmdDesc(CommandId{
				ClientId: propose.ClientId,
				SeqNum:   propose.CommandId,
			}))
		case m := <-r.cs.fastAckChan:
			fastAck := m.(*MFastAck)
			r.handleFastAck(fastAck, r.getCmdDesc(fastAck.CmdId))
		case m := <-r.cs.slowAckChan:
			slowAck := m.(*MSlowAck)
			r.handleSlowAck(slowAck, r.getCmdDesc(slowAck.CmdId))
		case m := <-r.cs.lightSlowAckChan:
			lightSlowAck := m.(*MLightSlowAck)
			r.handleLightSlowAck(lightSlowAck, r.getCmdDesc(lightSlowAck.CmdId))
		}
	}
}

func (r *ReplicaSim) handlePropose(p *genericsmr.Propose, desc *commandDescSim) {
	desc.propose = p
	if !r.AQ.Contains(r.Id) {
		desc.afterPropagate.Recall()
		return
	}
	if desc.afterPropagate.Recall() && desc.slowPath {
		return
	}
	dep := func() Dep {
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
	fastAck := &MFastAck{
		Replica: r.Id,
		Dep:     dep,
	}
	r.sender.SendToAll(fastAck, r.cs.fastAckRPC)
	r.handleFastAck(fastAck, desc)
}

func (r *ReplicaSim) handleFastAck(fastAck *MFastAck, desc *commandDescSim) {
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
			lightSlowAck := &MLightSlowAck{
				Replica: r.Id,
			}
			r.sender.SendToAll(lightSlowAck, r.cs.lightSlowAckRPC)
			r.handleLightSlowAck(lightSlowAck, desc)
		} else {
			desc.fastAndSlowAcks.Add(fastAck.Replica, true, fastAck)
		}
	})
}

func (r *ReplicaSim) commonCaseFastAck(fastAck *MFastAck, desc *commandDescSim) {
	desc.fastAndSlowAcks.Add(fastAck.Replica, false, fastAck)
}

func (r *ReplicaSim) handleSlowAck(slowAck *MSlowAck, desc *commandDescSim) {
	r.commonCaseFastAck((*MFastAck)(slowAck), desc)
}

func (r *ReplicaSim) handleLightSlowAck(lightSlowAck *MLightSlowAck, desc *commandDescSim) {
	r.commonCaseFastAck(&MFastAck{
		Replica: lightSlowAck.Replica,
		Dep:     nil,
	}, desc)
}

func (r *ReplicaSim) getCmdDesc(cmdId CommandId) *commandDescSim {
	desc, exists := r.cmdDescs[cmdId]
	if exists {
		return desc
	}
	desc = &commandDescSim{
		propose:  nil,
		slowPath: false,
	}

	acc := func(msg interface{}) bool {
		if desc.fastAndSlowAcks.leaderMsg == nil {
			return true
		}
		return msg.(*MFastAck).Dep == nil || goodDep.Equals(msg.(*MFastAck).Dep)
	}

	free := func(_ interface{}) {}

	fastAndSlowHandler := func(leaderMsg interface{}, msgs []interface{}) {
		if leaderMsg == nil {
			return
		}
		dCmdId := leaderMsg.(*MFastAck).CmdId
		dDesc := r.getCmdDesc(dCmdId)
		if dDesc.propose.Collocated {
			rep := &genericsmrproto.ProposeReplyTS{
				OK: genericsmr.TRUE,
				CommandId: dDesc.propose.CommandId,
				Value: state.NIL(),
				Timestamp: dDesc.propose.Timestamp,
			}
			r.ReplyProposeTS(rep, dDesc.propose.Reply, dDesc.propose.Mutex)
		}
	}

	desc.fastAndSlowAcks = NewMsgSet(r.AQ, acc, free, fastAndSlowHandler)
	desc.afterPropagate = NewOptCondF(func() bool {
		return desc.propose != nil
	})

	r.cmdDescs[cmdId] = desc
	return desc
}
