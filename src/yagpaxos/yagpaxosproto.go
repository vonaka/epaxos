package yagpaxos

import (
	"bytes"
	"fmt"
	"state"
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

func leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func inConflict(c1, c2 state.Command) bool {
	return state.Conflict(&c1, &c2)
}
