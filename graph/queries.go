package graph

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Passed to the logging thread.
type TimeseriesEntry[V VPI[V], E EPI[E], M MVI[M], N any] struct {
	Name             time.Time
	EdgeCount        uint64
	GraphView        *Graph[V, E, M, N]
	Latency          time.Duration
	CurrentRuntime   time.Duration
	AlgTimeSinceLast time.Duration
	AlgWaitGroup     *sync.WaitGroup
}

// Will check for any entry in the LogEntryChan.
// When one is supplied, it will copy the graph vertex properties, then can call "Finish" function for the algorithm.
// These properties are then sent to the applyTimeSeries func (defined by the algorithm).
func LogTimeSeries[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], entries chan TimeseriesEntry[V, E, M, N]) {
	algTimeLastLog := time.Duration(0)
	nQueries := uint64(0)
	allowAsyncProperties := g.Options.AllowAsyncVertexProps
	timings := g.Options.DebugLevel >= 2
	logConcChan := make(chan TimeseriesEntry[V, E, M, N])
	go QueryFinishConcurrent(alg, g, logConcChan, entries)
	currG := new(Graph[V, E, M, N])
	nextG := new(Graph[V, E, M, N])
	THREADS := g.NumThreads
	currG.NumThreads = g.NumThreads
	nextG.NumThreads = g.NumThreads
	for t := uint16(0); t < uint16(THREADS); t++ {
		currG.GraphThreads[t].Tidx = t
		nextG.GraphThreads[t].Tidx = t
	}
	tse := TimeseriesEntry[V, E, M, N]{}
	tse.AlgWaitGroup = new(sync.WaitGroup)

	for entry := range g.LogEntryChan {
		tse.Name = time.Unix(int64(entry), 0)
		algTimeNow := g.Watch.Elapsed()
		tse.CurrentRuntime = algTimeNow
		if !allowAsyncProperties {
			algTimeNow = g.AlgTimer.Pause() // To differentiate from time spent on alg, vs on query.
		}
		tse.AlgTimeSinceLast = algTimeNow - algTimeLastLog
		algTimeLastLog = algTimeNow

		// Note since the input stream is waiting for g.QueryWaiter.Done(), no new topology will come (though some may still be in process).
		if g.Options.AsyncContinuationTime > 0 {
			time.Sleep(time.Duration(g.Options.AsyncContinuationTime) * time.Millisecond)
		}

		m0 := time.Now()

		if g.Options.NoConvergeForQuery { // Copy the graph without full convergence.
			if QUERY_EMULATE_BSP { // Emulation of BSP framework.
				g.Broadcast(BSP_SYNC)
				g.AwaitAck()
			}

			// Block on current state.
			if !allowAsyncProperties {
				g.Broadcast(BLOCK_ALL) // Full, to both prevent both top and alg events (consistent view of vertex props).
			} else {
				g.Broadcast(BLOCK_TOP) // Enforce no topology changes but still allow alg convergence events.
			}
			g.AwaitAck()
		} else { // Copy the graph with full convergence. Broadcast EPOCH to await convergence.
			g.Broadcast(EPOCH)
			g.AwaitAck()
		}

		m1 := time.Now()

		tse.EdgeCount = uint64(currG.NodeParallelFor(func(_, _ uint32, gt *GraphThread[V, E, M, N]) int {
			original := &g.GraphThreads[gt.Tidx]
			original.NodeCopyVerticesInto(&gt.Vertices)     // Shallow-ish, full props but shallow edges.
			gt.VertexStructures = original.VertexStructures // Shallow copy the structure info.
			gt.NumEdges = original.NumEdges
			return int(gt.NumEdges)
		}))

		m2 := time.Now()

		if !allowAsyncProperties {
			g.AlgTimer.UnPause()
		}

		// Should be here
		if g.Options.OracleCompare {
			CompareToOracle(alg, g, true, false, false, false)
		}

		g.Broadcast(RESUME) // Have view of the graph, threads can continue now.

		nQueries++
		tse.GraphView = currG
		logConcChan <- tse
		currG, nextG = nextG, currG
		if timings {
			log.Trace().Msg(", query, " + utils.V(nQueries) + ", cmd, " + utils.F("%.3f", time.Duration(m1.Sub(m0)).Seconds()*1000) +
				", cpy, " + utils.F("%.3f", time.Duration(m2.Sub(m1)).Seconds()*1000) + ", chan, " + utils.F("%.3f", time.Since(m2).Seconds()*1000))
		}

		// Should be above with no-broadcast?
		//if g.Options.OracleCompare {
		//	CompareToOracle(alg, g, true, false, true, true)
		//}

		g.QueryWaiter.Done()
	}
	close(logConcChan)
}

// Finishes a graph if needed, then hands it off to the applyTimeSeries func (defined by the algorithm).
// This is done in a separate goroutine to allow for concurrent processing (the properties are copied).
func QueryFinishConcurrent[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], logConcChan chan TimeseriesEntry[V, E, M, N], entries chan TimeseriesEntry[V, E, M, N]) {
	nQueries := uint64(0)
	edgesLast := int64(0)
	sendTimeSeries := false
	finish := false

	finishFunc := func(g *Graph[V, E, M, N]) {} // No-op by default
	if aOF, ok := any(alg).(AlgorithmOnFinish[V, E, M, N]); ok {
		finishFunc = aOF.OnFinish
		finish = true
	}
	// If not implemented, we just print the stats, after everything is copied and finalized.
	if _, ok := any(alg).(AlgorithmOnApplyTimeSeries[V, E, M, N]); ok {
		sendTimeSeries = true
	}

	for tse := range logConcChan {
		preFinish := (g.Watch.Elapsed() - tse.CurrentRuntime)
		finishFunc(tse.GraphView)
		tse.Latency = (g.Watch.Elapsed() - tse.CurrentRuntime)
		nQueries++

		msg := ", query, " + utils.V(nQueries) + ", dE, " + utils.V(int64(tse.EdgeCount)-edgesLast)
		if finish {
			msg += ", preFinish, " + utils.F("%.3f", preFinish.Seconds()*1000)
		}
		msg += ", totalQuery, " + utils.F("%.3f", tse.Latency.Seconds()*1000) + ", algTimeSinceLast, " + utils.F("%.3f", tse.AlgTimeSinceLast.Seconds()*1000) + ", totalRuntime, " + utils.F("%.3f", tse.CurrentRuntime.Seconds()*1000)
		log.Info().Msg(msg)

		edgesLast = int64(tse.EdgeCount)
		if sendTimeSeries {
			tse.AlgWaitGroup.Add(1)
			entries <- tse
			tse.AlgWaitGroup.Wait()
		}
	}
	close(entries)
}
