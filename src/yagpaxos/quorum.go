package yagpaxos

type quorum map[int32]struct{}

type leaderQuorums map[int32]quorum

type quorumSet map[int32]leaderQuorums

func newQuorum(size int) quorum {
	return make(map[int32]struct{}, size)
}

func newQuorumOfAll(size int) quorum {
	q := newQuorum(size)

	for i := int32(0); i < int32(size); i++ {
		q[i] = struct{}{}
	}

	return q
}

func (q quorum) contains(repId int32) bool {
	_, exists := q[repId]
	return exists
}

func (q quorum) copy() quorum {
	nq := newQuorum(len(q))

	for cmdId := range q {
		nq[cmdId] = struct{}{}
	}

	return nq
}

func newLeaderQuorums() leaderQuorums {
	return make(map[int32]quorum)
}

func newQuorumSet(quorumSize, repNum int) quorumSet {
	ids := make([]int32, repNum)
	q := newQuorum(quorumSize)
	qs := make(map[int32]leaderQuorums, repNum)

	for id := range ids {
		ids[id] = int32(id)
		qs[int32(id)] = newLeaderQuorums()
	}

	subsets(ids, repNum, quorumSize, 0, q, qs)

	return qs
}

func (qs quorumSet) WQ(ballot int32) quorum {
	l := leader(ballot, len(qs))
	lqs := qs[l]
	qid := (ballot / int32(len(qs))) % int32(len(lqs))
	return lqs[qid]
}

type msgSet struct {
	q         quorum
	msgs      []interface{}
	leaderMsg interface{}
	accept    func(interface{}) bool
	handler   func(interface{}, []interface{})
}

func newMsgSet(q quorum, accept func(interface{}) bool,
	handler func(interface{}, []interface{})) *msgSet {

	return &msgSet{
		q:         q,
		msgs:      []interface{}{},
		leaderMsg: nil,
		accept:    accept,
		handler:   handler,
	}
}

func (ms *msgSet) add(repId int32, ballot int32,
	isLeader bool, msg interface{}) {
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

	// TODO: do not call this each time
	// (or maybe it's not that bad?)
	go ms.handler(ms.leaderMsg, ms.msgs)
}

func subsets(ids []int32, repNum, quorumSize, i int,
	q quorum, qs quorumSet) {

	if quorumSize == 0 {
		for repId := int32(0); repId < int32(repNum); repId++ {
			length := int32(len(qs[repId]))
			_, exists := q[repId]
			if exists {
				qs[repId][length] = q.copy()
			}
		}
	}

	for j := i; j < repNum; j++ {
		q[ids[j]] = struct{}{}
		subsets(ids, repNum, quorumSize-1, j+1, q, qs)
		delete(q, ids[j])
	}
}
