package yagpaxos

import (
	"genericsmr"
	"genericsmrproto"
	"state"
)

type replyArgs struct {
	val   state.Value
	desc  *commandDesc
	cmdId CommandId
}

type replyChan struct {
	args chan *replyArgs
	rep  *genericsmrproto.ProposeReplyTS
}

func NewReplyChan(r *Replica,
	after func(_ CommandId, _ *commandDesc)) *replyChan {
	rc := &replyChan{
		args: make(chan *replyArgs, 128),
		rep: &genericsmrproto.ProposeReplyTS{
			OK: genericsmr.TRUE,
		},
	}

	go func() {
		for !r.Shutdown {
			args := <-rc.args

			rc.rep.CommandId = args.desc.propose.CommandId
			rc.rep.Value = args.val
			rc.rep.Timestamp = args.desc.propose.Timestamp

			r.ReplyProposeTS(rc.rep, args.desc.propose.Reply,
				args.desc.propose.Mutex)
			after(args.cmdId, args.desc)
		}
	}()

	return rc
}

func (r *replyChan) reply(val state.Value, desc *commandDesc, cmdId CommandId) {
	r.args <- &replyArgs{
		val:   val,
		desc:  desc,
		cmdId: cmdId,
	}
}
