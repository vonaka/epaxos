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

func (cf *condF) call(f func()) bool {
	if cf.cond() {
		f()
		cf.fun = func() {}

		return true
	} else {
		oldFun := cf.fun
		cf.fun = func() {
			oldFun()
			f()
		}

		return false
	}
}

func (cf *condF) recall() bool {
	if cf.cond() {
		cf.fun()
		cf.fun = func() {}

		return true
	}

	return false
}

func (cf *condF) andCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() && cond()
	}
}

func (cf *condF) orCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() || cond()
	}
}
