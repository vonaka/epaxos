package yagpaxos

type msgSet struct {
	q         quorum
	msgs      []interface{}
	leaderMsg interface{}
	accept    func(interface{}) bool
	handler   func(interface{}, []interface{})
}

func newMsgSet(q quorum,
	accept func(interface{}) bool,
	handler func(interface{}, []interface{})) *msgSet {

	return &msgSet{
		q:         q,
		msgs:      []interface{}{},
		leaderMsg: nil,
		accept:    accept,
		handler:   handler,
	}
}

func (ms *msgSet) add(repId int32, ballot int32, isLeader bool, msg interface{}) {
	// TODO: check if msg is already in ms

	if !ms.q.contains(repId) {
		return
	}

	if isLeader {
		ms.leaderMsg = msg
		newMsgs := []interface{}{}
		for _, msg = range ms.msgs {
			if ms.accept(msg) {
				newMsgs = append(newMsgs, msg)
			}
		}
		ms.msgs = newMsgs
	} else if ms.accept(msg) {
		ms.msgs = append(ms.msgs, msg)
	}

	if len(ms.msgs) == len(ms.q) ||
		(len(ms.msgs) == len(ms.q)-1 && ms.leaderMsg != nil) {
		ms.handler(ms.leaderMsg, ms.msgs)
	}
}
