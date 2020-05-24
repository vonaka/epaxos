package yagpaxos

import (
	"fastrpc"
	"genericsmr"
)

const (
	SEND_ALL = iota
	SEND_QUORUM
	SEND_EXCEPT
	SEND_SINGLE
)

const ARGS_NUM int = genericsmr.CHAN_BUFFER_SIZE

type SendArg struct {
	msg      fastrpc.Serializable
	rpc      uint8
	quorum   Quorum
	sendType int32
}

type Sender chan SendArg

func NewSender(r *genericsmr.Replica) Sender {
	s := Sender(make(chan SendArg, ARGS_NUM))

	go func() {
		for !r.Shutdown {
			arg := <-s
			switch arg.sendType {
			case SEND_ALL:
				sendToAll(r, arg.msg, arg.rpc)
			case SEND_QUORUM:
				sendToQuorum(r, arg.quorum, arg.msg, arg.rpc)
			case SEND_EXCEPT:
				sendExcept(r, arg.quorum, arg.msg, arg.rpc)
			case SEND_SINGLE:
			}
		}
	}()

	return s
}

func (s Sender) SendToAll(msg fastrpc.Serializable, rpc uint8) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		sendType: SEND_ALL,
	}
}

func (s Sender) SendToQuorum(q Quorum, msg fastrpc.Serializable, rpc uint8) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		quorum:   q,
		sendType: SEND_QUORUM,
	}
}

func (s Sender) SendExcept(q Quorum, msg fastrpc.Serializable, rpc uint8) {
	s <- SendArg{
		msg:      msg,
		rpc:      rpc,
		quorum:   q,
		sendType: SEND_EXCEPT,
	}
}

func sendToAll(r *genericsmr.Replica, msg fastrpc.Serializable, rpc uint8) {
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

func sendToQuorum(r *genericsmr.Replica, q Quorum,
	msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		if !q.Contains(p) {
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

func sendExcept(r *genericsmr.Replica, q Quorum,
	msg fastrpc.Serializable, rpc uint8) {
	for p := int32(0); p < int32(r.N); p++ {
		if q.Contains(p) {
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
