package yagpaxos

type Quorum map[int32]struct{}

type QuorumsOfLeader map[int32]Quorum

type QuorumSet map[int32]QuorumsOfLeader

func NewQuorum(size int) Quorum {
	return make(map[int32]struct{}, size)
}

func NewQuorumOfAll(size int) Quorum {
	q := NewQuorum(size)

	for i := int32(0); i < int32(size); i++ {
		q[i] = struct{}{}
	}

	return q
}

func (q Quorum) Contains(repId int32) bool {
	_, exists := q[repId]
	return exists
}

func (q Quorum) copy() Quorum {
	nq := NewQuorum(len(q))

	for cmdId := range q {
		nq[cmdId] = struct{}{}
	}

	return nq
}

func NewQuorumsOfLeader() QuorumsOfLeader {
	return make(map[int32]Quorum)
}

func NewQuorumSet(quorumSize, repNum int) QuorumSet {
	ids := make([]int32, repNum)
	q := NewQuorum(quorumSize)
	qs := make(map[int32]QuorumsOfLeader, repNum)

	for id := range ids {
		ids[id] = int32(id)
		qs[int32(id)] = NewQuorumsOfLeader()
	}

	subsets(ids, repNum, quorumSize, 0, q, qs)

	return qs
}

func (qs QuorumSet) AQ(ballot int32) Quorum {
	l := leader(ballot, len(qs))
	lqs := qs[l]
	qid := (ballot / int32(len(qs))) % int32(len(lqs))
	return lqs[qid]
}

func subsets(ids []int32, repNum, quorumSize, i int,
	q Quorum, qs QuorumSet) {

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
