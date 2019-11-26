package yagpaxos

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fastrpc"
	"fmt"
	"io"
	"log"
	"state"
	"sync"
)

type CommandId struct {
	ClientId int32
	SeqNum   int32
}

func (cmdId CommandId) String() string {
	b := new(bytes.Buffer)
	fmt.Fprintf(b, "(client: %v, seq_num: %v)", cmdId.ClientId, cmdId.SeqNum)
	return b.String()
}

type Dep []CommandId

func (dep1 Dep) Equals(dep2 Dep) bool {
	if len(dep1) != len(dep2) {
		return false
	}

	seen1 := make(map[CommandId]struct{})
	seen2 := make(map[CommandId]struct{})
	for i := 0; i < len(dep1); i++ {
		if dep1[i] == dep2[i] {
			continue
		}

		_, exists := seen2[dep1[i]]
		if exists {
			delete(seen2, dep1[i])
		} else {
			seen1[dep1[i]] = struct{}{}
		}

		_, exists = seen1[dep2[i]]
		if exists {
			delete(seen1, dep2[i])
		} else {
			seen2[dep2[i]] = struct{}{}
		}
	}

	return len(seen1) == len(seen2) && len(seen1) == 0
}

func (dep1 Dep) EqualsAndDiff(dep2 Dep) (bool, map[CommandId]struct{}) {
	seen1 := make(map[CommandId]struct{})
	seen2 := make(map[CommandId]struct{})

	for i := 0; i < len(dep1) || i < len(dep2); i++ {
		if i < len(dep1) && i < len(dep2) && dep1[i] == dep2[i] {
			continue
		}

		if i < len(dep1) {
			_, exists := seen2[dep1[i]]
			if exists {
				delete(seen2, dep1[i])
			} else {
				seen1[dep1[i]] = struct{}{}
			}
		}

		if i < len(dep2) {
			_, exists := seen1[dep2[i]]
			if exists {
				delete(seen1, dep2[i])
			} else {
				seen2[dep2[i]] = struct{}{}
			}
		}
	}

	return len(dep1) == len(dep2) && len(seen1) == len(seen2) &&
		len(seen1) == 0, seen1
}

type keyInfo struct {
	getDep func() Dep
}

func newKeyInfo() *keyInfo {
	return &keyInfo{
		getDep: func() Dep {
			return []CommandId{}
		},
	}
}

func (ki *keyInfo) add(cmdId CommandId) {
	ki.getDep = func() Dep {
		return []CommandId{cmdId}
	}
}

func (ki *keyInfo) remove(cmdId CommandId) {
	dep := ki.getDep()

	// since inside one key each next cmd depends on previous one,
	// we suppose that if we want to remove latest id, then
	// it is also safe to remove all previous, hence:
	if len(dep) > 0 && cmdId == dep[0] {
		ki.getDep = func() Dep {
			return []CommandId{}
		}
	}
}

//////////////////////////////////////////////////////////////
//                                                          //
//  gobin-codegen doesn't support declarations of the form  //
//                                                          //
//      type A B                                            //
//                                                          //
//  that's why we use `[]CommandId` instead of `Dep`        //
//                                                          //
//////////////////////////////////////////////////////////////

type MFastAck struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Dep     []CommandId
}

type MSlowAck struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
	Dep     []CommandId
}

type MLightSlowAck struct {
	Replica int32
	Ballot  int32
	CmdId   CommandId
}

type MNewLeader struct {
	Replica int32
	Ballot  int32
}

type MNewLeaderAck struct {
	Replica int32
	Ballot  int32
	Cballot int32
	Phases  map[CommandId]int
	Cmds    map[CommandId]state.Command
	Deps    map[CommandId]Dep
}

type MSync struct {
	Replica int32
	Ballot  int32
	Phases  map[CommandId]int
	Cmds    map[CommandId]state.Command
	Deps    map[CommandId]Dep
}

type MSyncAck struct {
	Replica int32
	Ballot  int32
}

type MCollect struct {
	Replica int32
	CmdId   CommandId
}

type MFlush struct {
	Replica int32
	Ballot  int32
}

func (m *MFastAck) New() fastrpc.Serializable {
	return new(MFastAck)
}

func (m *MSlowAck) New() fastrpc.Serializable {
	return new(MSlowAck)
}

func (m *MLightSlowAck) New() fastrpc.Serializable {
	return new(MLightSlowAck)
}

func (m *MNewLeader) New() fastrpc.Serializable {
	return new(MNewLeader)
}

func (m *MNewLeaderAck) New() fastrpc.Serializable {
	return new(MNewLeaderAck)
}

func (m *MNewLeaderAck) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MNewLeaderAck) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

func (m *MSync) New() fastrpc.Serializable {
	return new(MSync)
}

func (m *MSync) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MSync) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

func (m *MSyncAck) New() fastrpc.Serializable {
	return new(MSyncAck)
}

func (m *MCollect) New() fastrpc.Serializable {
	return new(MCollect)
}

func (m *MFlush) New() fastrpc.Serializable {
	return new(MFlush)
}

func leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func inConflict(c1, c2 state.Command) bool {
	return state.Conflict(&c1, &c2)
}

///////////////////////////////////////////////////////////////////////////////
//                                                                           //
//  Generated with gobin-codegen [https://code.google.com/p/gobin-codegen/]  //
//                                                                           //
///////////////////////////////////////////////////////////////////////////////

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *MSyncAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type MSyncAckCache struct {
	mu    sync.Mutex
	cache []*MSyncAck
}

func NewMSyncAckCache() *MSyncAckCache {
	c := &MSyncAckCache{}
	c.cache = make([]*MSyncAck, 0)
	return c
}

func (p *MSyncAckCache) Get() *MSyncAck {
	var t *MSyncAck
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MSyncAck{}
	}
	return t
}
func (p *MSyncAckCache) Put(t *MSyncAck) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MSyncAck) Marshal(wire io.Writer) {
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

func (t *MSyncAck) Unmarshal(wire io.Reader) error {
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

func (t *MCollect) BinarySize() (nbytes int, sizeKnown bool) {
	return 12, true
}

type MCollectCache struct {
	mu    sync.Mutex
	cache []*MCollect
}

func NewMCollectCache() *MCollectCache {
	c := &MCollectCache{}
	c.cache = make([]*MCollect, 0)
	return c
}

func (p *MCollectCache) Get() *MCollect {
	var t *MCollect
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MCollect{}
	}
	return t
}
func (p *MCollectCache) Put(t *MCollect) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MCollect) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.Replica
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.ClientId
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *MCollect) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	return nil
}

func (t *MFlush) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type MFlushCache struct {
	mu    sync.Mutex
	cache []*MFlush
}

func NewMFlushCache() *MFlushCache {
	c := &MFlushCache{}
	c.cache = make([]*MFlush, 0)
	return c
}

func (p *MFlushCache) Get() *MFlush {
	var t *MFlush
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MFlush{}
	}
	return t
}
func (p *MFlushCache) Put(t *MFlush) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MFlush) Marshal(wire io.Writer) {
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

func (t *MFlush) Unmarshal(wire io.Reader) error {
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

func (t *CommandId) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type CommandIdCache struct {
	mu    sync.Mutex
	cache []*CommandId
}

func NewCommandIdCache() *CommandIdCache {
	c := &CommandIdCache{}
	c.cache = make([]*CommandId, 0)
	return c
}

func (p *CommandIdCache) Get() *CommandId {
	var t *CommandId
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &CommandId{}
	}
	return t
}
func (p *CommandIdCache) Put(t *CommandId) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *CommandId) Marshal(wire io.Writer) {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	tmp32 := t.ClientId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.SeqNum
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *CommandId) Unmarshal(wire io.Reader) error {
	var b [8]byte
	var bs []byte
	bs = b[:8]
	if _, err := io.ReadAtLeast(wire, bs, 8); err != nil {
		return err
	}
	t.ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.SeqNum = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	return nil
}

func (t *MFastAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MFastAckCache struct {
	mu    sync.Mutex
	cache []*MFastAck
}

func NewMFastAckCache() *MFastAckCache {
	c := &MFastAckCache{}
	c.cache = make([]*MFastAck, 0)
	return c
}

func (p *MFastAckCache) Get() *MFastAck {
	var t *MFastAck
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MFastAck{}
	}
	return t
}
func (p *MFastAckCache) Put(t *MFastAck) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MFastAck) Marshal(wire io.Writer) {
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
	tmp32 = t.CmdId.ClientId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Dep))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 = t.Dep[i].ClientId
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
		tmp32 = t.Dep[i].SeqNum
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (t *MFastAck) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Dep = make([]CommandId, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Dep[i].ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Dep[i].SeqNum = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (t *MSlowAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type MSlowAckCache struct {
	mu    sync.Mutex
	cache []*MSlowAck
}

func NewMSlowAckCache() *MSlowAckCache {
	c := &MSlowAckCache{}
	c.cache = make([]*MSlowAck, 0)
	return c
}

func (p *MSlowAckCache) Get() *MSlowAck {
	var t *MSlowAck
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MSlowAck{}
	}
	return t
}
func (p *MSlowAckCache) Put(t *MSlowAck) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MSlowAck) Marshal(wire io.Writer) {
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
	tmp32 = t.CmdId.ClientId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
	bs = b[:]
	alen1 := int64(len(t.Dep))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 = t.Dep[i].ClientId
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
		tmp32 = t.Dep[i].SeqNum
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
}

func (t *MSlowAck) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Dep = make([]CommandId, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Dep[i].ClientId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Dep[i].SeqNum = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	return nil
}

func (t *MLightSlowAck) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type MLightSlowAckCache struct {
	mu    sync.Mutex
	cache []*MLightSlowAck
}

func NewMLightSlowAckCache() *MLightSlowAckCache {
	c := &MLightSlowAckCache{}
	c.cache = make([]*MLightSlowAck, 0)
	return c
}

func (p *MLightSlowAckCache) Get() *MLightSlowAck {
	var t *MLightSlowAck
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MLightSlowAck{}
	}
	return t
}
func (p *MLightSlowAckCache) Put(t *MLightSlowAck) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MLightSlowAck) Marshal(wire io.Writer) {
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
	tmp32 = t.CmdId.ClientId
	bs[8] = byte(tmp32)
	bs[9] = byte(tmp32 >> 8)
	bs[10] = byte(tmp32 >> 16)
	bs[11] = byte(tmp32 >> 24)
	tmp32 = t.CmdId.SeqNum
	bs[12] = byte(tmp32)
	bs[13] = byte(tmp32 >> 8)
	bs[14] = byte(tmp32 >> 16)
	bs[15] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *MLightSlowAck) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.Replica = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.Ballot = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.CmdId.ClientId = int32((uint32(bs[8]) | (uint32(bs[9]) << 8) | (uint32(bs[10]) << 16) | (uint32(bs[11]) << 24)))
	t.CmdId.SeqNum = int32((uint32(bs[12]) | (uint32(bs[13]) << 8) | (uint32(bs[14]) << 16) | (uint32(bs[15]) << 24)))
	return nil
}

func (t *MNewLeader) BinarySize() (nbytes int, sizeKnown bool) {
	return 8, true
}

type MNewLeaderCache struct {
	mu    sync.Mutex
	cache []*MNewLeader
}

func NewMNewLeaderCache() *MNewLeaderCache {
	c := &MNewLeaderCache{}
	c.cache = make([]*MNewLeader, 0)
	return c
}

func (p *MNewLeaderCache) Get() *MNewLeader {
	var t *MNewLeader
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &MNewLeader{}
	}
	return t
}
func (p *MNewLeaderCache) Put(t *MNewLeader) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *MNewLeader) Marshal(wire io.Writer) {
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

func (t *MNewLeader) Unmarshal(wire io.Reader) error {
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
