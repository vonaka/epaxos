package yagpaxos

type msgSet struct {
	q         quorum
	msgs      []interface{}
	leaderMsg interface{}
	accept    func(interface{}) bool
	handler   func(interface{}, []interface{})
	deinit    func(interface{})
}

func newMsgSet(q quorum,
	accept func(interface{}) bool,
	handler func(interface{}, []interface{}),
	deinit func(interface{})) *msgSet {

	return &msgSet{
		q:         q,
		msgs:      []interface{}{},
		leaderMsg: nil,
		accept:    accept,
		handler:   handler,
		deinit:    deinit,
	}
}

func (ms *msgSet) add(repId, ballot int32, isLeader bool, msg interface{}) bool {
	// TODO: check if msg is already in ms

	added := false

	if !ms.q.contains(repId) {
		return added
	}

	if isLeader {
		ms.leaderMsg = msg
		newMsgs := []interface{}{}
		for _, msg = range ms.msgs {
			if ms.accept(msg) {
				newMsgs = append(newMsgs, msg)
			} else {
				ms.deinit(msg)
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

func (ms *msgSet) free() {
	for _, msg := range ms.msgs {
		ms.deinit(msg)
	}
	if ms.leaderMsg != nil {
		ms.deinit(ms.leaderMsg)
	}
}
