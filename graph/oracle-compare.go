package graph

import (
	"sync"

	"golang.org/x/exp/constraints"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Compares the current state of the graph to a computed oracle solution.
// TODO: we shallow copy edges, this would be a problem if the algorithm has edge properties...
func CompareToOracle[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], finishOriginal bool, cache bool, broadcast bool, syncWithQuery bool) {
	g.Watch.Pause()
	AlgTimerAlreadyPaused := g.AlgTimer.IsPaused()
	if !AlgTimerAlreadyPaused {
		g.AlgTimer.Pause()
	}

	if syncWithQuery { // Wait for all threads to sync to match the query before comparing to oracle
		g.Broadcast(TOP_SYNC)
		g.AwaitAck()
	}
	if broadcast {
		g.Broadcast(BLOCK_ALL)
		g.AwaitAck()
	}

	log.Info().Msg("----INLINE----")
	log.Debug().Msg("current time (ms): " + utils.V(g.Watch.Elapsed().Milliseconds()))

	var oracleGraph *Graph[V, E, M, N]

	numVertices := g.NodeVertexCount()

	maxAtEvent := uint64(0)
	for t := 0; t < int(g.NumThreads); t++ {
		maxAtEvent = utils.Max(maxAtEvent, g.GraphThreads[t].AtEvent)
	}

	if g.OracleCache == nil {
		log.Debug().Msg("Initializing oracle graph")
		oracleGraph = new(Graph[V, E, M, N])
		oracleGraph.NumThreads = g.NumThreads
		oracleGraph.Options = g.Options
		oracleGraph.Options.OracleCompare = false
		oracleGraph.Options.OracleCompareSync = false
		oracleGraph.Options.LogTimeseries = false
		oracleGraph.Options.TimeseriesEdgeCount = false
		oracleGraph.InitMails = g.InitMails
		oracleGraph.InitNotes = g.InitNotes
		oracleGraph.VertexMap = g.VertexMap
		oracleGraph.Init()

		log.Debug().Msg("Copying vertices: " + utils.V(numVertices))
		numEdges := 0
		for t := 0; t < int(g.NumThreads); t++ {
			// Ok to shallow copy, we do not edit.
			oracleGraph.GraphThreads[t].VertexStructures = g.GraphThreads[t].VertexStructures
			// Copy base structures.
			g.GraphThreads[t].NodeCopyVerticesInto(&oracleGraph.GraphThreads[t].Vertices)
			g.GraphThreads[t].NodeCopyVertexPropsInto(&oracleGraph.GraphThreads[t].VertexProperties)
			oracleGraph.GraphThreads[t].VertexMailboxes = g.GraphThreads[t].NodeCopyVertexMailboxes()
			oracleGraph.GraphThreads[t].NumEdges = g.GraphThreads[t].NumEdges
			oracleGraph.GraphThreads[t].NumOutDels = g.GraphThreads[t].NumOutDels
			oracleGraph.GraphThreads[t].AtEvent = g.GraphThreads[t].AtEvent
			numEdges += int(oracleGraph.GraphThreads[t].NumEdges)
		}

		oracleGraph.NodeForEachVertex(func(_, internalId uint32, oracleVertex *Vertex[V, E], oracleProp *V) {
			*oracleProp = (*oracleProp).New()
			mailbox, _ := oracleGraph.NodeVertexMailbox(internalId)
			mailbox.Inbox = mailbox.Inbox.New()
		})

		if oCopy, ok := any(new(V)).(VPCopyOracle[V]); ok {
			oracleGraph.NodeForEachVertex(func(_, internalId uint32, oracleVertex *Vertex[V, E], oracleProp *V) {
				oCopy.CopyForOracle(oracleProp, g.NodeVertexProperty(internalId))
			})
		}

		log.Info().Msg("Creating result for graph with " + utils.V(numVertices) + " vertices and " + utils.V(numEdges) + " edges")
		oracleGraph.AlgTimer.Start()
		ConvergeAsync(alg, oracleGraph, new(sync.WaitGroup))
		if aOF, ok := any(alg).(AlgorithmOnFinish[V, E, M, N]); ok {
			aOF.OnFinish(oracleGraph, oracleGraph, maxAtEvent)
		}
		msgSend := uint64(0)
		for t := 0; t < int(oracleGraph.NumThreads); t++ {
			msgSend += oracleGraph.GraphThreads[t].MsgSend
		}
		log.Info().Msg("Oracle Termination(ms) " + utils.V(oracleGraph.AlgTimer.Elapsed().Milliseconds()) +
			" Total(ms) " + utils.V(oracleGraph.Watch.Elapsed().Milliseconds()) + " Messages " + utils.V(msgSend))

		if cache {
			g.OracleCache = oracleGraph
		}
	} else {
		numEdges := 0
		for t := 0; t < int(g.NumThreads); t++ {
			numEdges += int(g.GraphThreads[t].NumEdges)
		}
		log.Info().Msg("Using cached result for graph with " + utils.V(numVertices) + " vertices and " + utils.V(numEdges) + " edges")
		oracleGraph = g.OracleCache
	}

	// Result complete, now compare.
	if aOOC, ok := any(alg).(AlgorithmOnOracleCompare[V, E, M, N]); ok {
		if aOF, ok := any(alg).(AlgorithmOnFinish[V, E, M, N]); ok {
			var gVertexPropStash []V
			if finishOriginal {
				gVertexPropStash = make([]V, numVertices)
				g.NodeForEachVertex(func(i, v uint32, vertex *Vertex[V, E], prop *V) {
					gVertexPropStash[i] = *prop
				})
				// Here we can "finish" proper G immediately for comparison (i.e., normalization / sink adjustment)
				// to compare a fully finished to the current state. Since the OnFinish is small in cost but big in effect,
				// important to compare with it applied to both.
				aOF.OnFinish(g, g, maxAtEvent)
			}

			aOOC.OnOracleCompare(g, oracleGraph)

			// Has to be here, before we reset properties.
			if g.Options.CheckCorrectness {
				OracleCheckCorrectness(alg, g, oracleGraph)
			}

			if finishOriginal {
				g.NodeForEachVertex(func(i, v uint32, vertex *Vertex[V, E], prop *V) {
					// Resetting the effect of the "early finish"
					*prop = gVertexPropStash[i]
				})
			}
		} else {
			aOOC.OnOracleCompare(g, oracleGraph)

			if g.Options.CheckCorrectness {
				OracleCheckCorrectness(alg, g, oracleGraph)
			}
		}
	} else {
		if g.Options.CheckCorrectness {
			log.Info().Msg("Algorithm does not support OnOracleCompare, but asked to. Continuing to check correctness.")
		} else {
			log.Warn().Msg("Algorithm does not support OnOracleCompare, but asked to; will check for correctness instead.")
			g.Options.CheckCorrectness = true
		}
		if _, ok := any(alg).(AlgorithmOnCheckCorrectness[V, E, M, N]); !ok {
			log.Panic().Msg("ERROR: Algorithm does not implement OnOracleCompare, or OnCheckCorrectness.")
		}
		OracleCheckCorrectness(alg, g, oracleGraph)
	}

	if !AlgTimerAlreadyPaused {
		g.AlgTimer.UnPause()
	}
	g.Watch.UnPause()
	log.Info().Msg("----END_INLINE----")
	if broadcast {
		g.ResetTerminationState()
		g.Broadcast(RESUME)
	}
}

func OracleCheckCorrectness[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], oracleGraph *Graph[V, E, M, N]) {
	if aOCC, ok := any(alg).(AlgorithmOnCheckCorrectness[V, E, M, N]); ok {
		log.Info().Msg("Checking correctness of oracle...")
		aOCC.OnCheckCorrectness(oracleGraph)

		log.Info().Msg("Checking correctness of given...")
		aOCC.OnCheckCorrectness(g)
	} else {
		log.Warn().Msg("WARNING: Algorithm does not implement OnCheckCorrectness, but asked to.")
	}
}

// Offers a generic comparison of vertex properties as sets of simple float or integer values.
// The single value to compare is dictated by the ValueOf function provided for a given VertexProperty.
// TODO: this is not very efficient, but works for now.
func OracleGenericCompareValues[V VPI[V], E EPI[E], M MVI[M], N any, VT constraints.Float | constraints.Integer, VO func(V) VT](g *Graph[V, E, M, N], oracle *Graph[V, E, M, N], ValueOf VO) {
	oracleValues := make([]VT, g.NodeVertexCount())
	givenGValues := make([]VT, g.NodeVertexCount())

	numEdges := g.NodeParallelFor(func(ordinalStart, _ uint32, givenGt *GraphThread[V, E, M, N]) int {
		oracleGt := &oracle.GraphThreads[givenGt.Tidx]
		for i := uint32(0); i < uint32(len(givenGt.Vertices)); i++ {
			givenProp := givenGt.VertexProperty(i)
			oracleProp := oracleGt.VertexProperty(i)
			oracleValues[ordinalStart+i] = ValueOf(*oracleProp)
			givenGValues[ordinalStart+i] = ValueOf(*givenProp)
		}
		return int(givenGt.NumEdges)
	})

	log.Info().Msg("VertexCount: " + utils.V(g.NodeVertexCount()) + " EdgeCount: " + utils.V(numEdges) + " Diffs:")
	avgL1Diff, medianL1Diff, percentile95L1 := utils.ResultCompare(oracleValues, givenGValues, 0)
	log.Info().Msg("AvgL1Diff " + utils.F("%.3e", avgL1Diff) + " MedianL1Diff " + utils.F("%.3e", medianL1Diff) + " 95pL1Diff " + utils.F("%.3e", percentile95L1))
}
