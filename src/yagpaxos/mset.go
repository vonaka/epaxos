package yagpaxos

type MsgSetHandler func(interface{}, []interface{})

type MsgSet struct {
	q         Quorum
	msgs      []interface{}
	leaderMsg interface{}
	accept    func(interface{}) bool
	handler   MsgSetHandler
}

func NewMsgSet(q Quorum,
	accept func(interface{}) bool, handler MsgSetHandler) *MsgSet {

	return &MsgSet{
		q:         q,
		msgs:      []interface{}{},
		leaderMsg: nil,
		accept:    accept,
		handler:   handler,
	}
}

func (ms *MsgSet) Add(repId int32, isLeader bool, msg interface{}) bool {

	if !ms.q.Contains(repId) {
		return false
	}

	added := false

	if isLeader {
		ms.leaderMsg = msg
		newMsgs := []interface{}{}
		for _, fmsg := range ms.msgs {
			if ms.accept(fmsg) {
				newMsgs = append(newMsgs, fmsg)
			}
		}
		ms.msgs = newMsgs
		added = true
	} else if ms.accept(msg) {
		ms.msgs = append(ms.msgs, msg)
		added = true
	}

	if len(ms.msgs) == len(ms.q) ||
		(len(ms.msgs) == len(ms.q)-1 && ms.leaderMsg != nil) {
		ms.handler(ms.leaderMsg, ms.msgs)
	}

	return added
}
