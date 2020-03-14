package yagpaxos

type msgSetHandler func(interface{}, []interface{})

type msgSet struct {
	q         quorum
	msgs      []interface{}
	leaderMsg interface{}
	accept    func(interface{}) bool
	handler   msgSetHandler
}

func newMsgSet(q quorum,
	accept func(interface{}) bool, handler msgSetHandler) *msgSet {

	return &msgSet{
		q:         q,
		msgs:      []interface{}{},
		leaderMsg: nil,
		accept:    accept,
		handler:   handler,
	}
}

func (ms *msgSet) add(repId int32, isLeader bool, msg interface{}) bool {

	if !ms.q.contains(repId) {
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
