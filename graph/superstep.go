package graph

type SuperStepWaiter struct {
	doneEvents   chan struct{}
	resumeEvents chan struct{}
}

func (ss *SuperStepWaiter) Init() {
	ss.doneEvents = make(chan struct{})
	ss.resumeEvents = make(chan struct{})
}

func AwaitSuperStepConvergence[V VPI[V], E EPI[E], M MVI[M], N any](alg Algorithm[V, E, M, N], g *Graph[V, E, M, N], tidx uint32) (completed bool) {
	ss := &g.SuperStepWaiter

	if tidx == 0 {
		for i := 1; i < int(g.NumThreads); i++ {
			<-ss.doneEvents
		}

		if aOSC, ok := alg.(AlgorithmOnSuperStepConverged[V, E, M, N]); ok {
			sent := aOSC.OnSuperStepConverged(g)
			if sent > 0 {
				g.GraphThreads[tidx].MsgSend += sent
				g.ResetTerminationState()
			}
		}

		for i := 1; i < int(g.NumThreads); i++ {
			ss.resumeEvents <- struct{}{}
		}
	} else {
		ss.doneEvents <- struct{}{}
		<-ss.resumeEvents
	}

	return g.CheckTermination(uint16(tidx))
}
