package yagpaxos

import (
	"sync"
	"sync/atomic"
	"time"
)

type ThroughputInfo struct {
	sync.Mutex
	init        bool
	startUpdate time.Time
	commitNum   int32
}

func NewThroughputInfo() *ThroughputInfo {
	return &ThroughputInfo{
		init:      false,
		commitNum: 0,
	}
}

func (ti *ThroughputInfo) AddCommit() {
	if ti.init {
		atomic.AddInt32(&ti.commitNum, 1)
	} else {
		ti.Lock()
		if !ti.init {
			ti.init = true
			ti.startUpdate = time.Now()
		}
		ti.Unlock()
		atomic.AddInt32(&ti.commitNum, 1)
	}
}

func (ti *ThroughputInfo) GetThroughput() float64 {
	commits := float64(atomic.LoadInt32(&ti.commitNum))
	return commits / time.Now().Sub(ti.startUpdate).Seconds()

	//commits := atomic.SwapInt32(&ti.commitNum, 0)
	//now := time.Now()
	//duration := now.Sub(ti.startUpdate)
	//ti.startUpdate = now
	//return float64(commits) / duration.Seconds()
}
