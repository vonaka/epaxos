package yagpaxos

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"
)

type committer struct {
	sync.Mutex

	next      int
	first     int
	last      int
	delivered int
	cmdIds    map[int]CommandId
	instances map[CommandId]int
}

func newCommitter(m *sync.Mutex, shutdown *bool) *committer {
	c := &committer{
		next:      0,
		first:     -1,
		last:      -1,
		delivered: -1,
		cmdIds:    make(map[int]CommandId, 10), // FIXME
		instances: make(map[CommandId]int, 10), // FIXME
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

func (c *committer) getInstance(cmdId CommandId) int {
	c.Lock()
	defer c.Unlock()
	return c.instances[cmdId]
}

func (c *committer) add(cmdId CommandId) {
	c.Lock()
	defer c.Unlock()

	_, exists := c.instances[cmdId]
	if exists {
		return
	}

	c.cmdIds[c.next] = cmdId
	c.instances[cmdId] = c.next
	c.last = c.next
	c.next++
}

func (c *committer) addTo(cmdId CommandId, instance int) {
	c.Lock()
	defer c.Unlock()

	c.cmdIds[instance] = cmdId
	c.instances[cmdId] = instance
	if instance > c.last {
		c.last = instance
	}
}

func (c *committer) deliver(cmdId CommandId, f func(CommandId) error) error {
	c.Lock()
	defer c.Unlock()

	i := c.delivered + 1
	j, exists := c.instances[cmdId]
	for ; exists && i <= j; i++ {
		if err := f(c.cmdIds[i]); err != nil {
			c.delivered = i - 1
			return err
		}
	}

	if exists && j > c.delivered {
		c.delivered = j
	}

	return nil
}

func (c *committer) safeDeliver(cmdId CommandId,
	f func(CommandId) error) error {
	c.Lock()
	defer c.Unlock()

	i := c.delivered + 1
	j, exists := c.instances[cmdId]
	for ; exists && i <= j; i++ {
		_, exists := c.cmdIds[i]
		if !exists {
			return errors.New("some dependency has not yet been committed")
		} else {
			if err := f(c.cmdIds[i]); err != nil {
				return err
			}
			c.delivered = i
		}
	}

	return nil
}

func (c *committer) wasDelivered(cmdId CommandId) bool {
	c.Lock()
	defer c.Unlock()

	i, exists := c.instances[cmdId]
	return exists && i <= c.delivered
}

func (c *committer) undeliveredIter(f func(CommandId)) {
	c.Lock()
	defer c.Unlock()

	i := c.delivered + 1
	j := c.last
	for ; i <= j; i++ {
		f(c.cmdIds[i])
	}
}

func (c *committer) String() string {
	b := new(bytes.Buffer)

	for i := c.first + 1; i < c.last; i++ {
		cmdId, exists := c.cmdIds[i]
		if !exists {
			fmt.Fprintf(b, "#\n")
		} else {
			fmt.Fprintf(b, "%v\n", cmdId)
		}
	}

	return b.String()
}

type commandIdList struct {
	cmdId    CommandId
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
	cmdDesc   map[CommandId]*cmdPos
}

func newIdList(cmdId CommandId) *commandIdList {
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
		cmdDesc:   make(map[CommandId]*cmdPos),
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

	b2.head = nil
	b2.tail = nil
}

func (b *committerBuilder) adjust(cmdId CommandId, dep DepVector) {
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

	dep.Iter(func(depCmdId CommandId) bool {
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
	oldCommitter.Lock()
	defer oldCommitter.Unlock()

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
		fmt.Fprintf(buffer, "%v: %v, ", list.index, list.cmdId)
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
