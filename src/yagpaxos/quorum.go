package yagpaxos

import (
	"sync"
	"time"
)

const INITIAL_Q_SIZE = 20

type quorum struct {
	size     int
	leaderId int
	elements []interface{}
}

type quorumSet struct {
	sync.Mutex
	mainQuorum *quorum
	quorums    []*quorum
	realLen    int
	related    func(interface{}, interface{}) bool
	strongTest func(*quorum) bool
	weakTest   func(*quorum) bool
	handler    func(quorum)
	afterHours bool
}

func newQuorum() *quorum {
	return &quorum{
		size:     0,
		leaderId: -1,
		elements: make([]interface{}, INITIAL_Q_SIZE),
	}
}

func (q *quorum) add(e interface{}, fromLeader bool) {
	if q.size < len(q.elements) {
		q.elements[q.size] = e
	} else {
		q.elements = append(q.elements, e)
	}
	if fromLeader && q.leaderId == -1 {
		q.leaderId = q.size
	}
	q.size++
}

func (q *quorum) getLeaderElement() interface{} {
	if q.leaderId != -1 {
		return q.elements[q.leaderId]
	}
	return nil
}

func newQuorumSet(related func(interface{}, interface{}) bool,
	strongTest func(*quorum) bool, weakTest func(*quorum) bool,
	handler func(quorum), waitFor time.Duration) *quorumSet {
	qs := &quorumSet{
		mainQuorum: newQuorum(),
		quorums:    make([]*quorum, INITIAL_Q_SIZE),
		realLen:    0,
		related:    related,
		strongTest: strongTest,
		weakTest:   weakTest,
		handler:    handler,
		afterHours: false,
	}

	go func() {
		time.Sleep(waitFor)
		qs.Lock()
		defer qs.Unlock()
		if qs.afterHours {
			return
		}
		qs.afterHours = true
		if qs.weakTest(qs.mainQuorum) {
			go qs.handler(*qs.mainQuorum)
		}
	}()

	return qs
}

func (qs *quorumSet) add(e interface{}, fromLeader bool) {
	qs.Lock()
	defer qs.Unlock()

	qs.mainQuorum.add(e, fromLeader)
	if qs.afterHours {
		if qs.weakTest(qs.mainQuorum) {
			go qs.handler(*qs.mainQuorum)
		}
		return
	}

	for i := 0; i < qs.realLen; i++ {
		q := qs.quorums[i]
		if qs.related(q.elements[0], e) {
			q.add(e, fromLeader)
			if !qs.afterHours && qs.strongTest(q) {
				go qs.handler(*q)
			}
			return
		}
	}

	q := newQuorum()
	q.add(e, fromLeader)

	if qs.realLen < len(qs.quorums) {
		qs.quorums[qs.realLen] = q
	} else {
		qs.quorums = append(qs.quorums, q)
	}
	qs.realLen++

	if !qs.afterHours && qs.strongTest(q) {
		go qs.handler(*q)
	}
}
