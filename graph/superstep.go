package graph

type SuperStepWaiter struct {
	doneMessages   chan struct{}
	resumeMessages chan struct{}
}

func (ss *SuperStepWaiter) Init(numThreads int) {
	ss.doneMessages = make(chan struct{}, numThreads)
	ss.resumeMessages = make(chan struct{}, numThreads)
}

func AwaitSuperStepConvergence[V VPI[V], E EPI[E], M MVI[M], N any](alg Algorithm[V, E, M, N], g *Graph[V, E, M, N], tidx uint32) (completed bool) {
	ss := &g.SuperStepWaiter

	if tidx == 0 {
		for i := 1; i < int(g.NumThreads); i++ {
			<-ss.doneMessages
		}

		aOSC, _ := alg.(AlgorithmOnSuperStepConverged[V, E, M, N])
		sent := aOSC.OnSuperStepConverged(g)
		if sent > 0 {
			g.GraphThreads[tidx].MsgSend += sent
			g.ResetTerminationState()
		}

		for i := 1; i < int(g.NumThreads); i++ {
			ss.resumeMessages <- struct{}{}
		}
	} else {
		ss.doneMessages <- struct{}{}
		<-ss.resumeMessages
	}

	return g.CheckTermination(uint16(tidx))
}
