package yagpaxos

import (
	"bytes"
	"errors"
	"fmt"
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

func (c *committer) deliver(cmdId int32, f func(int32) error) {
	i := c.delivered + 1
	j, exists := c.instances[cmdId]
	for ; exists && i <= j; i++ {
		if f(c.cmdIds[i]) != nil {
			c.delivered = i - 1
			return
		}
	}

	if exists && j > c.delivered {
		c.delivered = j
	}
}

func (c *committer) safeDeliver(cmdId int32, f func(int32) error) error {
	i := c.delivered + 1
	j, exists := c.instances[cmdId]
	for ; exists && i <= j; i++ {
		_, exists := c.cmdIds[i]
		if !exists {
			return errors.New("some dependency is not commited yet")
		} else {
			if f(c.cmdIds[i]) != nil {
				return nil
			}
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

type commandIdList struct {
	cmdId    int32
	index    int
	next     *commandIdList
	previous *commandIdList
}

type buildingBlock struct {
	nextBlock     *buildingBlock
	previousBlock *buildingBlock
	head          *commandIdList
	tail          *commandIdList
}

type cmdPos struct {
	block *buildingBlock
	list  *commandIdList
}

type committerBuilder struct {
	headBlock *buildingBlock
	cmdDesc   map[int32]*cmdPos
}

func newIdList(cmdId int32) *commandIdList {
	return &commandIdList{
		cmdId:    cmdId,
		index:    0,
		next:     nil,
		previous: nil,
	}
}

func (l *commandIdList) remove() {
	if l == nil {
		return
	}

	if l.previous != nil {
		l.previous.next = l.next
	}
	if l.next != nil {
		l.next.previous = l.previous
	}

	list := l.next
	for list != nil {
		list.index--
		list = list.next
	}

	l.index = 0
	l.next = nil
	l.previous = nil
}

func (l *commandIdList) before(l2 *commandIdList) {
	if l == nil || l2 == nil {
		return
	}

	l.index = l2.index
	l.previous = l2.previous
	if l.previous != nil {
		l.previous.next = l
	}

	l.next = l2
	l2.previous = l

	list := l.next
	for list != nil {
		list.index++
		list = list.next
	}
}

func newBuilder() *committerBuilder {
	return &committerBuilder{
		headBlock: nil,
		cmdDesc:   make(map[int32]*cmdPos),
	}
}

func (b *committerBuilder) join(b1, b2 *buildingBlock) {
	if b2.previousBlock != nil {
		b2.previousBlock.nextBlock = b2.nextBlock
	}
	if b2.nextBlock != nil {
		b2.nextBlock.previousBlock = b2.previousBlock
	}
	b2.nextBlock = nil
	b2.previousBlock = nil

	b1.tail.next = b2.head
	b2.head.previous = b1.tail

	tailIndex := b1.tail.index
	list := b2.head
	for list != nil {
		list.index += (tailIndex + 1)
		b.cmdDesc[list.cmdId].block = b1
		list = list.next
	}

	b1.tail = b2.tail

	if b.headBlock == b2 {
		b.headBlock = b1
	}
}

func (b *committerBuilder) adjust(cmdId int32, dep yagpaxosproto.DepVector) {
	var block *buildingBlock
	var list *commandIdList

	cmdInfo, exists := b.cmdDesc[cmdId]
	if !exists {
		list = newIdList(cmdId)

		block = &buildingBlock{
			nextBlock:     b.headBlock,
			previousBlock: nil,
			head:          list,
			tail:          list,
		}
		if b.headBlock != nil {
			b.headBlock.previousBlock = block
		}
		b.headBlock = block
		b.cmdDesc[cmdId] = &cmdPos{
			block: block,
			list:  list,
		}
	} else {
		block = cmdInfo.block
		list = cmdInfo.list
	}

	dep.Iter(func(depCmdId int32) bool {
		depCmdInfo, exists := b.cmdDesc[depCmdId]

		if exists {
			depBlock := depCmdInfo.block
			depList := depCmdInfo.list

			if depBlock != block {
				b.join(depBlock, block)
				block = depBlock
			} else if list.index < depList.index {
				if block.tail == depList {
					block.tail = depList.previous
				}
				depList.remove()
				depList.before(list)
				if block.head == list {
					block.head = depList
				}
			}
		} else {
			depList := newIdList(depCmdId)
			depList.before(list)
			if block.head == list {
				block.head = depList
			}
			b.cmdDesc[depCmdId] = &cmdPos{
				block: block,
				list:  depList,
			}
		}

		return false
	})
}

func (builder *committerBuilder) buildCommitterFrom(oldCommitter *committer,
	m *sync.Mutex, shutdown *bool) *committer {
	block := builder.headBlock
	committer := newCommitter(m, shutdown)
	for block != nil {
		list := block.head
		for list != nil {
			cmdId := list.cmdId
			instance, exists := oldCommitter.instances[cmdId]
			if exists && instance <= oldCommitter.delivered {
				list = list.next
				continue
			}
			committer.add(cmdId)
			list = list.next
		}
		block = block.nextBlock
	}

	return committer
}

func (list *commandIdList) String() string {
	buffer := new(bytes.Buffer)

	fmt.Fprintf(buffer, "[ ")
	for list != nil {
		fmt.Fprintf(buffer, "%v, ", list.cmdId)
		list = list.next
	}
	fmt.Fprintf(buffer, "]")

	return buffer.String()
}

func (builder *committerBuilder) String() string {
	buffer := new(bytes.Buffer)
	block := builder.headBlock

	fmt.Fprintf(buffer, "*  ")
	for block != nil {
		fmt.Fprintf(buffer, "%v\n|_ ", block.head)
		block = block.nextBlock
	}
	fmt.Fprintf(buffer, "<>")

	return buffer.String()
}
