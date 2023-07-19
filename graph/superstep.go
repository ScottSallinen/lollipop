package graph

import (
	"sync"
)

type SuperStepWaiter struct {
	lock      sync.Mutex
	cond      sync.Cond
	doneCount int
}

func (ss *SuperStepWaiter) Init() {
	ss.cond.L = &ss.lock
}

func AwaitSuperStepConvergence[V VPI[V], E EPI[E], M MVI[M], N any](alg Algorithm[V, E, M, N], g *Graph[V, E, M, N], tidx uint32) (completed bool) {
	ss := &g.SuperStepWaiter
	ss.lock.Lock()

	ss.doneCount++
	if ss.doneCount != int(g.NumThreads) {
		ss.cond.Wait()
	} else {
		if aOSC, ok := alg.(AlgorithmOnSuperStepConverged[V, E, M, N]); ok {
			sent := aOSC.OnSuperStepConverged(g)
			if sent > 0 {
				g.GraphThreads[tidx].MsgSend += sent
				g.ResetTerminationState()
			}
		}
		ss.doneCount = 0
		ss.cond.Broadcast()
	}

	ss.lock.Unlock()
	return g.CheckTermination(uint16(tidx))
}
