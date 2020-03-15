package yagpaxos

import (
	"bufio"
	"encoding/binary"
	"fastrpc"
	"io"
	"state"
	"sync"
)

type M2A struct {
	Replica int32
	Ballot  int32
	Cmd     state.Command
	CmdSlot int
}

type M2B struct {
	Replica int32
	Ballot  int32
	CmdSlot int
}

type M1A struct {
	Replica int32
	Ballot  int32
}

type M1B struct {
	Replica int32
	Ballot  int32
	CBallot int32
	Cmds    []state.Command
}

type MPaxosSync struct {
	Replica int32
	Ballot  int32
	Cmds    []state.Command
}

///////////////////////////////////////////////////////////////////////////////
//                                                                           //
//  Generated with gobin-codegen [https://code.google.com/p/gobin-codegen/]  //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

func (m *M1A) New() fastrpc.Serializable {
	return new(M1A)
}

func (m *M1B) New() fastrpc.Serializable {
	return new(M1B)
}

func (m *M2A) New() fastrpc.Serializable {
	return new(M2A)
}

func (m *M2B) New() fastrpc.Serializable {
	return new(M2B)
}

func (m *MPaxosSync) New() fastrpc.Serializable {
	return new(MPaxosSync)
}

func (t *M2B) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type M2BCache struct {
	mu    sync.Mutex
	cache []*M2B
}

func NewM2BCache() *M2BCache {
	c := &M2BCache{}
	c.cache = make([]*M2B, 0)
	return c
}

func (p *M2BCache) Get() *M2B {
	var t *M2B
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &M2B{}
	}
	return t
}
func (p *M2BCache) Put(t *M2B) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *M2B) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.CmdSlot
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *M2B) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdSlot = int((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *M1A) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type M1ACache struct {
	mu    sync.Mutex
	cache []*M1A
}

func NewM1ACache() *M1ACache {
	c := &M1ACache{}
	c.cache = make([]*M1A, 0)
	return c
}

func (p *M1ACache) Get() *M1A {
	var t *M1A
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &M1A{}
	}
	return t
}
func (p *M1ACache) Put(t *M1A) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *M1A) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *M1A) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *M1B) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type M1BCache struct {
	mu    sync.Mutex
	cache []*M1B
}

func NewM1BCache() *M1BCache {
	c := &M1BCache{}
	c.cache = make([]*M1B, 0)
	return c
}

func (p *M1BCache) Get() *M1B {
	var t *M1B
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &M1B{}
	}
	return t
}
func (p *M1BCache) Put(t *M1B) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *M1B) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CBallot
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Cmds))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmds[i].Marshal(wire)
	}
}

func (t *M1B) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CBallot = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmds = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmds[i].Unmarshal(wire)
	}
	return nil
}

func (t *MPaxosSync) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MPaxosSyncCache struct {
	mu    sync.Mutex
	cache []*MPaxosSync
}

func NewMPaxosSyncCache() *MPaxosSyncCache {
	c := &MPaxosSyncCache{}
	c.cache = make([]*MPaxosSync, 0)
	return c
}

func (p *MPaxosSyncCache) Get() *MPaxosSync {
	var t *MPaxosSync
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MPaxosSync{}
	}
	return t
}
func (p *MPaxosSyncCache) Put(t *MPaxosSync) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MPaxosSync) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Cmds))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		t.Cmds[i].Marshal(wire)
	}
}

func (t *MPaxosSync) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Cmds = make([]state.Command, alen1)
	for i := int64(0); i < alen1; i++ {
		t.Cmds[i].Unmarshal(wire)
	}
	return nil
}

func (t *M2A) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type M2ACache struct {
	mu    sync.Mutex
	cache []*M2A
}

func NewM2ACache() *M2ACache {
	c := &M2ACache{}
	c.cache = make([]*M2A, 0)
	return c
}

func (p *M2ACache) Get() *M2A {
	var t *M2A
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &M2A{}
	}
	return t
}
func (p *M2ACache) Put(t *M2A) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *M2A) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.Ballot
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
	t.Cmd.Marshal(wire)
	tmp64 := t.CmdSlot
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *M2A) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.Cmd.Unmarshal(wire)
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.CmdSlot = int((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
	return nil
}
