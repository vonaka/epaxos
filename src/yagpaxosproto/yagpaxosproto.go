package yagpaxosproto

import (
	"encoding/gob"
	"fastrpc"
	"io"
	"log"
	"state"
)

type MFastAck struct {
	Replica  int32
	Ballot   int32
	Instance int32
	Dep      DepSet
}

type MCommit struct {
	Replica  int32
	Instance int32
	Command  state.Command
	Dep      DepSet
}

type MSlowAck struct {
	Replica  int32
	Ballot   int32
	Instance int32
	Command  state.Command
	Dep      DepSet
}

type MNewLeader struct {
	Replica int32
	Ballot  int32
}

type MNewLeaderAck struct {
	Replica int32
	Ballot  int32
	Cballot int32
	Phases  map[int32]int
	Cmds    map[int32]state.Command
	Deps    map[int32]DepSet
}

type MSync struct {
	Replica int32
	Ballot  int32
	Phases  map[int32]int
	Cmds    map[int32]state.Command
	Deps    map[int32]DepSet
}

type MSyncAck struct {
	Replica int32
	Ballot  int32
}

func (m *MFastAck) New() fastrpc.Serializable {
	return new(MFastAck)
}

func (m *MFastAck) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MFastAck) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

func (m *MCommit) New() fastrpc.Serializable {
	return new(MCommit)
}

func (m *MCommit) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MCommit) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

func (m *MSlowAck) New() fastrpc.Serializable {
	return new(MSlowAck)
}

func (m *MSlowAck) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MSlowAck) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

func (m *MNewLeader) New() fastrpc.Serializable {
	return new(MNewLeader)
}

func (m *MNewLeader) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MNewLeader) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
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

func (m *MSyncAck) Marshal(w io.Writer) {
	e := gob.NewEncoder(w)

	err := e.Encode(m)
	if err != nil {
		log.Fatal("Don't know what to do")
	}
}

func (m *MSyncAck) Unmarshal(r io.Reader) error {
	e := gob.NewDecoder(r)

	return e.Decode(m)
}

type DepSet map[int32]struct{}

func NewDepSet() DepSet {
	return make(DepSet)
}

func (d DepSet) Add(cmdId int32) {
	d[cmdId] = struct{}{}
}

func (d1 DepSet) Equals(d2 DepSet) bool {
	if len(d1) != len(d2) {
		return false
	}

	for cmdId, _ := range d1 {
		_, exists := d1[cmdId]
		if !exists {
			return false
		}
	}

	return true
}
