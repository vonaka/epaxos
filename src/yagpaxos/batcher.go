package yagpaxos

type Batcher struct {
	fastAcks      chan *MFastAck
	lightSlowAcks chan *MLightSlowAck
}

func NewBatcher(r *Replica, size int,
	freeFastAck func(*MFastAck), freeSlowAck func(*MLightSlowAck)) *Batcher {
	b := &Batcher{
		fastAcks:      make(chan *MFastAck, size),
		lightSlowAcks: make(chan *MLightSlowAck, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case fastAck := <-b.fastAcks:
				fLen := len(b.fastAcks) + 1
				sLen := len(b.lightSlowAcks)
				acks := &MAcks{
					FastAcks:      make([]MFastAck, fLen),
					LightSlowAcks: make([]MLightSlowAck, sLen),
				}

				acks.FastAcks[0] = *fastAck
				freeFastAck(fastAck)
				for i := 1; i < fLen; i++ {
					f := <-b.fastAcks
					acks.FastAcks[i] = *f
					freeFastAck(f)
				}
				for i := 0; i < sLen; i++ {
					s := <-b.lightSlowAcks
					acks.LightSlowAcks[i] = *s
					freeSlowAck(s)
				}
				r.sender.SendToAll(acks, r.cs.acksRPC)

			case slowAck := <-b.lightSlowAcks:
				fLen := len(b.fastAcks)
				sLen := len(b.lightSlowAcks) + 1
				acks := &MAcks{
					FastAcks:      make([]MFastAck, fLen),
					LightSlowAcks: make([]MLightSlowAck, sLen),
				}

				for i := 0; i < fLen; i++ {
					f := <-b.fastAcks
					acks.FastAcks[i] = *f
					freeFastAck(f)
				}
				acks.LightSlowAcks[0] = *slowAck
				freeSlowAck(slowAck)
				for i := 1; i < sLen; i++ {
					s := <-b.lightSlowAcks
					acks.LightSlowAcks[i] = *s
					freeSlowAck(s)
				}
				r.sender.SendToAll(acks, r.cs.acksRPC)
			}
		}
	}()

	return b
}

func (b *Batcher) SendFastAck(f *MFastAck) {
	b.fastAcks <- f
}

func (b *Batcher) SendLightSlowAck(s *MLightSlowAck) {
	b.lightSlowAcks <- s
}
