package graph

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Basic sync emulation. Runs through the vertex list.
// Note that vertices early in the list may affect vertices later in the list (activate and send messages)
// So this is not exactly emulating traditional BSP -- for that see below.
func ConvergeSync[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	SendInitialMessages(alg, g)
	if g.Options.OracleCompareSync {
		CompareToOracle(alg, g, true, true, false, false)
	}
	iteration := 1
	vertexUpdates := make([]int, g.NumThreads)

	for ; ; iteration++ {
		activity := g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) (tActivity int) {
			var msg M
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				vertex, vtm := gt.VertexAndMessages(i)
				active := atomic.SwapInt32(&vtm.Activity, 0) == 1
				if active {
					gt.NotificationQueue.Accept() // must exist, should discard
					msg = alg.MessageRetrieve(&vtm.Inbox, vertex)
					sent := alg.OnUpdateVertex(g, vertex, Notification[N]{Target: (i | threadOffset)}, msg)
					tActivity += int(sent)
					gt.MsgSend += sent
					vertexUpdates[gt.Tidx]++
				}
			}
			return tActivity
		})
		if g.Options.OracleCompareSync {
			CompareToOracle(alg, g, true, true, false, false)
		}
		if activity == 0 {
			break
		}
	}
	log.Info().Msg("Iterations: " + utils.V(iteration) + " Updates: " + utils.V(utils.Sum(vertexUpdates)))
}

// A sync variant that only only consumes messages generated from the previous iteration.
// Does not consider messages generated from the current iteration.
func ConvergeSyncPrevOnly[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	SendInitialMessages(alg, g)
	if g.Options.OracleCompareSync {
		CompareToOracle(alg, g, true, true, false, false)
	}
	iteration := 1
	vertexUpdates := make([]int, g.NumThreads)
	numVertices := g.NodeVertexCount()
	msgsLast := make([]M, numVertices)
	frontier := make([]bool, numVertices)

	for ; ; iteration++ {
		g.NodeParallelFor(func(ordinalStart, _ uint32, gt *GraphThread[V, E, M, N]) int {
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				vertex, vtm := gt.VertexAndMessages(i)
				active := atomic.SwapInt32(&vtm.Activity, 0) == 1
				if active {
					gt.NotificationQueue.Accept() // must exist, should discard
					msgsLast[ordinalStart+i] = alg.MessageRetrieve(&vtm.Inbox, vertex)
					frontier[ordinalStart+i] = true
				}
			}
			return 0
		})
		activity := g.NodeParallelFor(func(ordinalStart, threadOffset uint32, gt *GraphThread[V, E, M, N]) (tActivity int) {
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				idx := ordinalStart + i
				if frontier[idx] {
					frontier[idx] = false
					vertexUpdates[gt.Tidx]++
					sent := alg.OnUpdateVertex(g, gt.Vertex(i), Notification[N]{Target: (i | threadOffset)}, msgsLast[idx])
					tActivity += int(sent)
					gt.MsgSend += sent
				}
			}
			return tActivity
		})

		if g.Options.OracleCompareSync {
			CompareToOracle(alg, g, true, true, false, false)
		}
		if activity == 0 {
			break
		}
	}
	log.Info().Msg("Iterations: " + utils.V(iteration) + " Updates: " + utils.V(utils.Sum(vertexUpdates)))
}
