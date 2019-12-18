package yagpaxos

import "sync"

var WAIT_FOR = 800

type gc struct {
	sync.Mutex
	clean  func(CommandId)
	cmds   map[CommandId]map[int32]struct{}
	trash  map[CommandId]struct{}
	wakeup chan struct{}
}

func newGc(clean func(CommandId)) *gc {
	g := gc{
		clean:  clean,
		cmds:   make(map[CommandId]map[int32]struct{}),
		trash:  make(map[CommandId]struct{}, WAIT_FOR),
		wakeup: make(chan struct{}, 2),
	}

	return &g
}

func (g *gc) check() {
	select {
	case <-g.wakeup:
		g.Lock()
		for cmdId := range g.trash {
			g.clean(cmdId)
			delete(g.trash, cmdId)
		}
		g.Unlock()
	default:
	}
}

func (g *gc) collect(cmdId CommandId, replicaId int32, totalReplicaNum int) {
	g.Lock()
	defer g.Unlock()

	rs, exists := g.cmds[cmdId]
	if !exists {
		g.cmds[cmdId] = make(map[int32]struct{}, totalReplicaNum)
		rs = g.cmds[cmdId]
	}

	_, exists = rs[replicaId]
	if exists {
		return
	}

	rs[replicaId] = struct{}{}
	if len(rs) == totalReplicaNum {
		g.trash[cmdId] = struct{}{}
		if len(g.trash) == WAIT_FOR {
			g.wakeup <- struct{}{}
		}
	}
}
