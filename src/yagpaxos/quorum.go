package yagpaxos

type quorum map[int32]struct{}

type quorumSet struct {
	actualQuorum int
	quorums      map[int]quorum
}

func newQuorum(size int) quorum {
	return make(map[int32]struct{}, size)
}

func (q quorum) copy() quorum {
	nq := newQuorum(len(q))

	for cmdId := range q {
		nq[cmdId] = struct{}{}
	}

	return nq
}

func newQuorumSet(quorumSize, repNum int) *quorumSet {
	ids := make([]int32, repNum)
	q := newQuorum(quorumSize)
	quorums := make(map[int]quorum)

	for id := range ids {
		ids[id] = int32(id)
	}

	return &quorumSet{
		actualQuorum: 0,
		quorums:      subset(ids, repNum, quorumSize, 0, q, quorums),
	}
}

func subset(ids []int32, repNum, quorumSize, i int,
	q quorum, quorums map[int]quorum) map[int]quorum {

	if quorumSize == 0 {
		quorums[len(quorums)] = q.copy()
		return quorums
	}

	for j := i; j < repNum; j++ {
		q[ids[j]] = struct{}{}
		subset(ids, repNum, quorumSize-1, j+1, q, quorums)
		delete(q, ids[j])
	}

	return quorums
}
