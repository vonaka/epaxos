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
	Dep      map[int32]bool
}

type MCommit struct {
	Replica  int32
	Instance int32
	Command  state.Command
	Dep      map[int32]bool
}

type MSlowAck struct {
	Replica  int32
	Ballot   int32
	Instance int32
	Command  state.Command
	Dep      map[int32]bool
}

type MNewLeader struct {
	Replica int32
	Ballot  int32
}

type MNewLeaderAck struct {
	Replica int32
	Ballot  int32
	Cballot int32
	Phase   int
	Command state.Command
	Dep     map[int32]bool
}

type MSync struct {
	Replica int32
	Ballot  int32
	Phase   int
	Cmds    map[int32]state.Command
	Deps    map[int32](map[int32]bool)
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
