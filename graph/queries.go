package graph

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Passed to the logging thread.
type TimeseriesEntry[V VPI[V], E EPI[E], M MVI[M], N any] struct {
	Name             time.Time
	EdgeCount        uint64
	EdgeDeletes      uint64
	AtEventIndex     uint64
	GraphView        *Graph[V, E, M, N]
	Latency          time.Duration
	CurrentRuntime   time.Duration
	AlgTimeSinceLast time.Duration
}

// Will check for any entry in the LogEntryChan.
// When one is supplied, it will copy the graph vertex properties, then can call "Finish" function for the algorithm.
// These properties are then sent to the applyTimeSeries func (defined by the algorithm).
func LogTimeSeries[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	algTimeNow := time.Duration(0)
	algTimeLast := time.Duration(0)
	nQueries := uint64(0)
	edgesLast := int64(0)
	allowAsyncProperties := g.Options.AllowAsyncVertexProps
	algTimeIncludeQuery := g.Options.AlgTimeIncludeQuery
	ConcurrentFinishChan := make(chan TimeseriesEntry[V, E, M, N])
	mustFinish := false
	if _, mustFinish = any(alg).(AlgorithmOnFinish[V, E, M, N]); mustFinish {
		go QueryFinishConcurrent(alg, g, ConcurrentFinishChan) // Will use concurrent finish.
	}
	currG := &Graph[V, E, M, N]{NumThreads: g.NumThreads}
	nextG := &Graph[V, E, M, N]{NumThreads: g.NumThreads}
	for t := uint16(0); t < uint16(g.NumThreads); t++ {
		currG.GraphThreads[t].Tidx = t
		nextG.GraphThreads[t].Tidx = t
	}
	tse := TimeseriesEntry[V, E, M, N]{}
	var m0, m1, m2, m3 time.Time

	for entry := range g.LogEntryChan {
		tse.Name = time.Unix(int64(entry), 0)
		tse.CurrentRuntime = g.Watch.Elapsed()

		if !algTimeIncludeQuery {
			if allowAsyncProperties {
				algTimeNow = tse.CurrentRuntime
			} else {
				algTimeNow = g.AlgTimer.Pause()
			}
			tse.AlgTimeSinceLast = algTimeNow - algTimeLast
			algTimeLast = algTimeNow
		}

		// Note since the input stream is waiting for g.QueryWaiter.Done(), no new topology will come (though some may still be in process).
		if g.Options.AsyncContinuationTime > 0 {
			time.Sleep(time.Duration(g.Options.AsyncContinuationTime) * time.Millisecond)
		}

		m0 = time.Now()

		if g.Options.NoConvergeForQuery { // View the graph without full convergence.
			// Block on current state.
			if !allowAsyncProperties {
				g.Broadcast(BLOCK_ALL) // Full, to both prevent both top and alg events (consistent view of vertex props).
			} else {
				g.Broadcast(BLOCK_TOP) // Enforce no topology changes but still allow alg convergence events.
			}
			g.AwaitAck()
		} else { // View the graph after full convergence. Broadcast EPOCH to await convergence.
			g.Broadcast(EPOCH)
			g.AwaitAck()
		}

		if algTimeIncludeQuery {
			if allowAsyncProperties {
				algTimeNow = tse.CurrentRuntime
			} else {
				algTimeNow = g.AlgTimer.Pause()
			}
			tse.AlgTimeSinceLast = algTimeNow - algTimeLast
			algTimeLast = algTimeNow
		}

		m1 = time.Now()

		tse.EdgeCount = 0
		tse.EdgeDeletes = 0
		for t := 0; t < int(g.NumThreads); t++ {
			tse.EdgeDeletes += uint64(g.GraphThreads[t].NumOutDels)
			tse.EdgeCount += uint64(g.GraphThreads[t].NumEdges)
			tse.AtEventIndex = utils.Max(tse.AtEventIndex, g.GraphThreads[t].AtEvent)
		}

		if mustFinish {
			tse.GraphView = currG
			for t := 0; t < int(g.NumThreads); t++ {
				viewT := &tse.GraphView.GraphThreads[t]
				originalT := &g.GraphThreads[viewT.Tidx]
				originalT.NodeAllocatePropertyBuckets(&viewT.VertexProperties)
				viewT.Vertices = originalT.Vertices                 // Shallow copy the vertices.
				viewT.VertexStructures = originalT.VertexStructures // Shallow copy the extra structure info.
				viewT.NumEdges = originalT.NumEdges
			}
			m2 = time.Now()

			ConcurrentFinishChan <- tse
			currG, nextG = nextG, currG
			m3 = time.Now()
		} else {
			m2 = time.Now()
			tse.GraphView = g
			QueryFinalize(alg, g, tse, nQueries, edgesLast, "")
			m3 = time.Now()
		}

		if g.Options.OracleCompare {
			if allowAsyncProperties {
				g.AlgTimer.Pause()
			}
			CompareToOracle(alg, g, true, false, false, false)
			g.AlgTimer.UnPause()
		} else if !allowAsyncProperties {
			g.AlgTimer.UnPause()
		}

		g.ResetTerminationState()
		g.Broadcast(RESUME) // Have view of the graph, threads can continue now.

		nQueries++
		edgesLast = int64(tse.EdgeCount)
		if g.Options.DebugLevel >= 2 {
			log.Trace().Msg(", query, " + utils.V(nQueries) + ", command, " + utils.F("%.3f", m1.Sub(m0).Seconds()*1000) +
				", copy, " + utils.F("%.3f", m2.Sub(m1).Seconds()*1000) + ", finish, " + utils.F("%.3f", m3.Sub(m2).Seconds()*1000) + ", resume, " + utils.F("%.3f", time.Since(m3).Seconds()*1000))
		}

		g.QueryWaiter.Done()
	}
	close(ConcurrentFinishChan)
}

// Finishes a graph, then hands it off to the applyTimeSeries func (defined by the algorithm).
// This is done in a separate goroutine to allow for concurrent processing.
func QueryFinishConcurrent[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], ConcurrentFinishChan chan TimeseriesEntry[V, E, M, N]) {
	nQueries := uint64(0)
	edgesLast := int64(0)

	if aOF, ok := any(alg).(AlgorithmOnFinish[V, E, M, N]); ok {
		for tse := range ConcurrentFinishChan {
			preFinish := (g.Watch.Elapsed() - tse.CurrentRuntime)
			aOF.OnFinish(g, tse.GraphView, tse.AtEventIndex)
			finishStr := ", preFinish, " + utils.F("%.3f", preFinish.Seconds()*1000)

			QueryFinalize(alg, g, tse, nQueries, edgesLast, finishStr)
			nQueries++
			edgesLast = int64(tse.EdgeCount)
		}
	} else {
		log.Panic().Msg("Concurrent finish requested on algorithm that does not implement OnFinish")
	}
}

// Hands off to the applyTimeSeries func (defined by the algorithm).
func QueryFinalize[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], tse TimeseriesEntry[V, E, M, N], nQueries uint64, edgesLast int64, finishStr string) {
	toPrint := ", query, " + utils.V(nQueries) + ", dE, " + utils.V(int64(tse.EdgeCount)-edgesLast) + finishStr

	tse.Latency = (g.Watch.Elapsed() - tse.CurrentRuntime)
	toPrint += ", totalQuery, " + utils.F("%.3f", tse.Latency.Seconds()*1000) + ", algTimeSinceLast, " + utils.F("%.3f", tse.AlgTimeSinceLast.Seconds()*1000) + ", totalRuntime, " + utils.F("%.3f", tse.CurrentRuntime.Seconds()*1000)
	log.Info().Msg(toPrint)

	if aTS, ok := any(alg).(AlgorithmOnApplyTimeSeries[V, E, M, N]); ok {
		aTS.OnApplyTimeSeries(tse)
	}
}
