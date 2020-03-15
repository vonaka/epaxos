package yagpaxos

import (
	"genericsmr"
	"genericsmrproto"
	"state"
)

type replyArgs struct {
	val     state.Value
	propose *genericsmr.Propose
	finish  chan interface{}
	cmdId   CommandId
}

type replyChan struct {
	args chan *replyArgs
	rep  *genericsmrproto.ProposeReplyTS
}

func NewReplyChan(r *Replica) *replyChan {
	rc := &replyChan{
		args: make(chan *replyArgs, 128),
		rep: &genericsmrproto.ProposeReplyTS{
			OK: genericsmr.TRUE,
		},
	}

	go func() {
		slot := 0
		for !r.Shutdown {
			args := <-rc.args

			rc.rep.CommandId = args.propose.CommandId
			rc.rep.Value = args.val
			rc.rep.Timestamp = args.propose.Timestamp

			r.ReplyProposeTS(rc.rep, args.propose.Reply, args.propose.Mutex)
			args.finish <- slot
			slot = (slot + 1) % HISTORY_SIZE
		}
	}()

	return rc
}

func (r *replyChan) reply(val state.Value, desc *commandDesc, cmdId CommandId) {
	r.args <- &replyArgs{
		val:     val,
		propose: desc.propose,
		cmdId:   cmdId,
		finish:  desc.msgs,
	}
}
