package yagpaxos

import "sync"

const WAIT_FOR = 800

type gc struct {
	sync.Mutex
	clean  func(int32)
	cmds   map[int32]map[int32]struct{}
	trash  map[int32]struct{}
	wakeup chan struct{}
}

func newGc(clean func(int32), mutex *sync.Mutex, shutdown *bool) *gc {
	g := gc{
		clean:  clean,
		cmds:   make(map[int32]map[int32]struct{}),
		trash:  make(map[int32]struct{}, WAIT_FOR),
		wakeup: make(chan struct{}, 1),
	}

	go func(g *gc) {
		for !*shutdown {
			<-g.wakeup
			g.Lock()
			mutex.Lock()
			for cmdId := range g.trash {
				clean(cmdId)
				delete(g.trash, cmdId)
			}
			mutex.Unlock()
			g.Unlock()
		}
	}(&g)

	return &g
}

func (g *gc) collect(cmdId, replicaId int32, totalReplicaNum int) {
	rs, exists := g.cmds[cmdId]
	if !exists {
		g.cmds[cmdId] = make(map[int32]struct{})
		rs = g.cmds[cmdId]
	}

	_, exists = rs[replicaId]
	if exists {
		return
	}

	rs[replicaId] = struct{}{}
	if len(rs) == totalReplicaNum {
		go func(g *gc) {
			g.Lock()
			defer g.Unlock()
			g.trash[cmdId] = struct{}{}
			if len(g.trash) == WAIT_FOR {
				g.wakeup <- struct{}{}
			}
		}(g)
	}
}
