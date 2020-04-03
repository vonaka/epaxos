package yagpaxos

type CondF struct {
	cond  func() bool
	fun   func()
	wanna bool
}

func NewCondF(cond func() bool) *CondF {
	return &CondF{
		cond:  cond,
		fun:   func() {},
		wanna: false,
	}
}

func (cf *CondF) Call(f func()) bool {
	if cf.cond() {
		f()
		cf.fun = func() {}
		cf.wanna = false

		return true
	} else {
		oldFun := cf.fun
		cf.fun = func() {
			oldFun()
			f()
		}
		cf.wanna = true

		return false
	}
}

func (cf *CondF) Recall() bool {
	if cf.wanna && cf.cond() {
		cf.fun()
		cf.fun = func() {}
		cf.wanna = false

		return true
	}

	return false
}

func (cf *CondF) AndCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() && cond()
	}
}

func (cf *CondF) OrCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() || cond()
	}
}
