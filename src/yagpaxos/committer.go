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
	id          int
	blockLeader int32
	cmds        []int32
	nextBlock   *buildingBlock
}

type committerBuilder struct {
	headBlock *buildingBlock
	tailBlock *buildingBlock
	blocks    map[int32]*buildingBlock
	maxId     int
}

func newBuilderFromCommitter(c *committer) *committerBuilder {
	block := &buildingBlock{
		id:          0,
		blockLeader: c.cmdIds[c.last],
		cmds:        make([]int32, c.last-c.first),
		nextBlock:   nil,
	}
	builder := &committerBuilder{
		headBlock: block,
		tailBlock: block,
		blocks:    make(map[int32]*buildingBlock),
		maxId:     0,
	}

	for i := c.first + 1; i <= c.last; i++ {
		block.cmds[i] = c.cmdIds[i]
		builder.blocks[c.cmdIds[i]] = block
	}

	return builder
}

func (b *committerBuilder) adjust(cmdId int32, dep yagpaxosproto.DepSet) {
	block, exists := b.blocks[cmdId]
	if !exists {
		block = &buildingBlock{
			id:          b.maxId + 1,
			blockLeader: cmdId,
			cmds:        []int32{},
			nextBlock:   nil,
		}
		b.maxId++
		b.tailBlock.nextBlock = block
		b.tailBlock = block
		mergeWith := block
		dep.Iter(func(cmdId int32) bool {
			depBlock, exists := b.blocks[cmdId]
			if exists && depBlock.id < mergeWith.id {
				mergeWith = depBlock
			} else if !exists {
				block.cmds = append(block.cmds, cmdId)
				b.blocks[cmdId] = block
			}
			return false
		})
		block.cmds = append(block.cmds, cmdId)
		b.blocks[cmdId] = block
		nextBlock := mergeWith.nextBlock
		for nextBlock != nil {
			mergeWith.blockLeader = nextBlock.blockLeader
			for _, id := range nextBlock.cmds {
				if id != -1 {
					mergeWith.cmds = append(mergeWith.cmds, id)
					b.blocks[id] = mergeWith
				}
			}
			nextBlock = nextBlock.nextBlock
		}
		mergeWith.nextBlock = nil
		b.maxId = mergeWith.id
		b.tailBlock = mergeWith
	} else {
		// split
	}
}
