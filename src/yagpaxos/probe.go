package yagpaxos

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type probe struct {
	name string
	n    int64

	totalDuration   time.Duration
	minDuration     time.Duration
	maxDuration     time.Duration

	startTime time.Time
}

func newProbe(name string) *probe {
	p := &probe{
		name:        name,
		minDuration: -1,
	}

	user1 := make(chan os.Signal, 1)
	signal.Notify(user1, syscall.SIGUSR1)

	go func() {
		<-user1
		fmt.Println(p)
	}()

	return p
}

func (p *probe) start() {
	p.startTime = time.Now()
}

func (p *probe) stop() {
	endTime := time.Now()
	duration := endTime.Sub(p.startTime)

	if duration > p.maxDuration {
		p.maxDuration = duration
	}

	if duration < p.minDuration || p.minDuration == -1 {
		p.minDuration = duration
	}

	p.totalDuration = p.totalDuration + duration
	p.n++
}

func (p *probe) String() string {
	average := time.Duration(0)
	if p.n != 0 {
		average = p.totalDuration / time.Duration(p.n)
	}

	return fmt.Sprintf("%s\n" +
		"total duration:   %v\n" +
		"average duration: %v\n" +
		"min duration:     %v\n" +
		"max duration:     %v\n" +
		"number of calls:  %v\n",
		p.name, p.totalDuration, average,
		p.minDuration, p.maxDuration, p.n)
}

