package yagpaxos

import (
	"errors"
	"sync"
	"time"
	"yagpaxosproto"
)

type committer struct {
	next      int
	first     int
	last      int
	delivered int
	cmdIds    map[int]int32
	instances map[int32]int
}

func newCommitter(m *sync.Mutex, shutdown *bool) *committer {
	c := &committer{
		next:      0,
		first:     -1,
		last:      -1,
		delivered: -1,
		cmdIds:    make(map[int]int32, 10), // FIXME
		instances: make(map[int32]int, 10), // FIXME
	}

	go func() {
		for !*shutdown {
			time.Sleep(8 * time.Second) // FIXME
			m.Lock()
			for i := c.first + 1; i <= c.delivered; i++ {
				// delete(c.instances, c.cmdIds[i])
				// we can't delete c.instance so simply,
				// as it can be used in getInstance
				delete(c.cmdIds, i)
			}
			c.first = c.delivered
			m.Unlock()
		}
	}()

	return c
}

func (c *committer) getInstance(cmdId int32) int {
	return c.instances[cmdId]
}

func (c *committer) add(cmdId int32) {
	c.cmdIds[c.next] = cmdId
	c.instances[cmdId] = c.next
	c.last = c.next
	c.next++
}

func (c *committer) addTo(cmdId int32, instance int) {
	c.cmdIds[instance] = cmdId
	c.instances[cmdId] = instance
	if instance > c.last {
		c.last = instance
	}
}

func (c *committer) deliver(cmdId int32, f func(int32)) {
	i := c.delivered + 1
	j := c.instances[cmdId]
	for ; i <= j; i++ {
		f(c.cmdIds[i])
	}

	if j > c.delivered {
		c.delivered = j
	}
}

func (c *committer) safeDeliver(cmdId int32, cmdDeps yagpaxosproto.DepSet,
	f func(int32)) error {
	i := c.delivered + 1
	j := c.instances[cmdId]
	for ; i <= j; i++ {
		_, exists := c.cmdIds[i]
		if !exists {
			return errors.New("some dependency is no commited yet")
		} else {
			f(c.cmdIds[i])
			c.delivered = i
		}
	}

	return nil
}

func (c *committer) wasDelivered(cmdId int32) bool {
	i, exists := c.instances[cmdId]
	return exists && i <= c.delivered
}

func (c *committer) undeliveredIter(f func(int32)) {
	i := c.delivered + 1
	j := c.last
	for ; i <= j; i++ {
		f(c.cmdIds[i])
	}
}

type buildingBlock struct {
	cmds          []int32
	nextBlock     *buildingBlock
	previousBlock *buildingBlock
}

type cmdPos struct {
	block *buildingBlock
	id    int
}

type committerBuilder struct {
	headBlock *buildingBlock
	cmdDesc   map[int32]*cmdPos
}

func newBuilder() *committerBuilder {
	return &committerBuilder{
		headBlock: nil,
		cmdDesc:   make(map[int32]*cmdPos),
	}
}

func (b1 *buildingBlock) putInFrontOf(b2 *buildingBlock) {
	if b2.nextBlock != nil {
		b2.nextBlock.previousBlock = b2.previousBlock
	}
	if b2.previousBlock != nil {
		b2.previousBlock.nextBlock = b2.nextBlock
	}

	b2.nextBlock = b1.nextBlock
	if b1.nextBlock != nil {
		b1.nextBlock.previousBlock = b2
	}

	b2.previousBlock = b1
	b1.nextBlock = b2
}

func (builder *committerBuilder) merge(b1 *buildingBlock, b2 *buildingBlock) {
	b1.putInFrontOf(b2)

	b1Len := len(b1.cmds)
	for i, id := range b2.cmds {
		b1.cmds = append(b1.cmds, id)
		builder.cmdDesc[id].block = b1
		builder.cmdDesc[id].id = i + b1Len
	}
	b1.nextBlock = b2.nextBlock
	if b1.nextBlock != nil {
		b1.nextBlock.previousBlock = b1
	}

	if builder.headBlock == b2 {
		builder.headBlock = b1
	}
}

func (b *committerBuilder) adjust(cmdId int32, dep yagpaxosproto.DepSet) {
	var block *buildingBlock
	id := 0

	cmdInfo, exists := b.cmdDesc[cmdId]
	if !exists {
		block = &buildingBlock{
			cmds:          []int32{cmdId},
			nextBlock:     b.headBlock,
			previousBlock: nil,
		}
		if b.headBlock != nil {
			b.headBlock.previousBlock = block
		}
		b.headBlock = block
		b.cmdDesc[cmdId] = &cmdPos{
			block: block,
			id:    0,
		}
	} else {
		block = cmdInfo.block
		id = cmdInfo.id
	}

	beforeBlockLeader := []int32{}
	afterBlockLeader := []int32{}
	copy(beforeBlockLeader, block.cmds[:id])
	copy(afterBlockLeader, block.cmds[id:])
	newDepId := len(beforeBlockLeader)
	dep.Iter(func(depCmdId int32) bool {
		depCmdInfo, exists := b.cmdDesc[depCmdId]

		if exists {
			depBlock := depCmdInfo.block
			depId := depCmdInfo.id

			if depBlock != block {
				block.cmds = append(beforeBlockLeader, afterBlockLeader...)
				for i := range afterBlockLeader {
					b.cmdDesc[afterBlockLeader[i]].id = i + newDepId
				}
				b.merge(depBlock, block)
				block = depBlock
				id += newDepId
				beforeBlockLeader = []int32{}
				afterBlockLeader = []int32{}
				copy(beforeBlockLeader, block.cmds[:id])
				copy(afterBlockLeader, block.cmds[id:])
				newDepId = len(beforeBlockLeader)
			} else if depId > id {
				beforeBlockLeader = append(beforeBlockLeader, depCmdId)
				b.cmdDesc[cmdId] = &cmdPos{
					block: block,
					id: newDepId,
				}
				newDepId++
				afterBlockLeader1 := []int32{}
				afterBlockLeader2 := []int32{}
				copy(afterBlockLeader1, afterBlockLeader[:depId])
				copy(afterBlockLeader2, afterBlockLeader[(depId + 1):])
				afterBlockLeader =
					append(afterBlockLeader1, afterBlockLeader2...)
			}
		} else {
			beforeBlockLeader = append(beforeBlockLeader, depCmdId)
			b.cmdDesc[cmdId] = &cmdPos{
				block: block,
				id: newDepId,
			}
			newDepId++
		}
		return false
	})

	block.cmds = append(beforeBlockLeader, afterBlockLeader...)
	for i := range afterBlockLeader {
		b.cmdDesc[afterBlockLeader[i]].id = i + newDepId
	}
}

func (builder *committerBuilder) buildCommitterFrom(oldCommitter *committer,
	m *sync.Mutex, shutdown *bool) *committer{
	block := builder.headBlock
	committer := newCommitter(m, shutdown)
	for block != nil {
		for i := range block.cmds {
			instance, exists := oldCommitter.instances[block.cmds[i]]
			if exists && instance <= oldCommitter.delivered {
				continue
			}
			committer.add(block.cmds[i])
		}
	}
	return committer
}
