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

type keyInfo struct {
	clientLastWrite []CommandId
	clientLastCmd   []CommandId
	lastWriteIndex  map[int32]int
	lastCmdIndex    map[int32]int
}

func (ki *keyInfo) add(cmd state.Command, cmdId CommandId) {
	cmdIndex, exists := ki.lastCmdIndex[cmdId.ClientId]

	if exists {
		ki.clientLastCmd[cmdIndex] = cmdId
	} else {
		ki.lastCmdIndex[cmdId.ClientId] = len(ki.clientLastCmd)
		ki.clientLastCmd = append(ki.clientLastCmd, cmdId)
	}

	if cmd.Op == state.PUT {
		writeIndex, exists := ki.lastWriteIndex[cmdId.ClientId]

		if exists {
			ki.clientLastWrite[writeIndex] = cmdId
		} else {
			ki.lastWriteIndex[cmdId.ClientId] = len(ki.clientLastWrite)
			ki.clientLastWrite = append(ki.clientLastWrite, cmdId)
		}
	}
}

func leader(ballot int32, repNum int) int32 {
	return ballot % int32(repNum)
}

func inConflict(c1, c2 state.Command) bool {
	return state.Conflict(&c1, &c2)
}
