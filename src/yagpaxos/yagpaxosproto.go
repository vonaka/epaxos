package yagpaxos

import (
	"bytes"
	"fmt"
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
