package yagpaxos

import (
	"sync"
	"time"
)

type quorum struct {
	// not the desired size, but the actual one
	size     int
	leaderId int
	elements []interface{}
}

type quorumSet struct {
	sync.Mutex
	neededSize     int
	totalSize      int
	quorums        map[int]*quorum
	leaderQuorum   *quorum
	biggestQuorum  *quorum
	related        func(interface{}, interface{}) bool
	wakeupIf       func() bool
	out            chan struct{}
	stop           chan struct{}
	called         bool
	onlyWithLeader bool
}

func newQuorumSet(size int, waitFor time.Duration,
	relation func(interface{}, interface{}) bool,
	wakeupIf func() bool, handler func(*quorum),
	onlyWithLeader bool) *quorumSet {
	qs := &quorumSet{
		neededSize:     size,
		totalSize:      0,
		quorums:        make(map[int]*quorum, size),
		leaderQuorum:   nil,
		biggestQuorum:  nil,
		related:        relation,
		wakeupIf:       wakeupIf,
		out:            make(chan struct{}, size),
		stop:           make(chan struct{}, 1),
		called:         false,
		onlyWithLeader: onlyWithLeader,
	}

	go func() {
		stop := false
		for !stop {
			select {
			case <-qs.out:
				var q *quorum
				if qs.onlyWithLeader {
					qs.Lock()
					q = qs.leaderQuorum
					qs.Unlock()
				} else {
					qs.Lock()
					q = qs.biggestQuorum
					qs.Unlock()
				}
				handler(q)
			case <-qs.stop:
				var q *quorum
				if qs.onlyWithLeader {
					qs.Lock()
					q = qs.leaderQuorum
					qs.Unlock()
				} else {
					qs.Lock()
					q = qs.biggestQuorum
					qs.Unlock()
				}
				handler(q)
				stop = true
			}
		}
	}()

	go func() {
		time.Sleep(waitFor)
		qs.stop <- struct{}{}
	}()

	return qs
}

func (q *quorum) getLeaderMsg() interface{} {
	if q.leaderId != -1 {
		return q.elements[q.leaderId]
	}
	return nil
}

func (qs *quorumSet) add(e interface{}, fromLeader bool) {
	qs.Lock()
	defer qs.Unlock()

	if qs.neededSize < 1 {
		if !qs.called && qs.wakeupIf() &&
			(!qs.onlyWithLeader || qs.leaderQuorum != nil) {
			qs.called = true
			qs.out <- struct{}{}
		}
		return
	}

	// TODO: deal with duplicates
	qs.totalSize++

	if qs.called {
		return
	}

	for i := 0; i < len(qs.quorums); i++ {
		q := qs.quorums[i]
		if qs.related(q.elements[0], e) {
			if q.size >= qs.neededSize {
				q.elements = append(q.elements, e)
			} else {
				q.elements[q.size] = e
			}
			if fromLeader && q.leaderId == -1 {
				q.leaderId = q.size
				qs.leaderQuorum = q
			}
			q.size++
			if qs.biggestQuorum == nil || q.size > qs.biggestQuorum.size {
				qs.biggestQuorum = q
			}
			if q.size >= qs.neededSize && qs.wakeupIf() &&
				(!qs.onlyWithLeader || qs.leaderQuorum != nil) {
				qs.called = true
				qs.out <- struct{}{}
			}
			return
		}
	}

	i := len(qs.quorums)
	q := &quorum{
		size:     1,
		leaderId: -1,
		elements: make([]interface{}, qs.neededSize),
	}
	qs.quorums[i] = q

	q.elements[0] = e
	if fromLeader {
		q.leaderId = 0
		qs.leaderQuorum = q
	}
	qs.biggestQuorum = q

	if qs.neededSize < 2 && qs.wakeupIf() &&
		(!qs.onlyWithLeader || qs.leaderQuorum != nil) {
		qs.called = true
		qs.out <- struct{}{}
	}
}
