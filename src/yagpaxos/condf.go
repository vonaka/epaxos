package yagpaxos

type condF struct {
	cond func() bool
	fun  func()
}

func newCondF(cond func() bool) *condF {
	return &condF{
		cond: cond,
		fun:  func() {},
	}
}

func (cf *condF) call(f func()) {
	if cf.cond() {
		f()
		cf.fun = func() {}
	} else {
		oldFun := cf.fun
		cf.fun = func() {
			oldFun()
			f()
		}
	}
}

func (cf *condF) recall() {
	if cf.cond() {
		cf.fun()
		cf.fun = func() {}
	}
}

func (cf *condF) andCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() && cond()
	}
}
