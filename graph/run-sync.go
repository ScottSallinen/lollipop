package graph

import (
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Basic sync emulation. Runs through the list of vertices in the graph in order (specifically the vertices of the graph thread, per thread).
// Note that vertices early in the list  may target vertices later in that list (activate and send notifications or mail to them).
// This new data may be viewed by that target, and hence this is actually semi-asynchronous -- but "fair", as vertices are looked at round-robin for an "iteration".
// Thus, this is not exactly emulating traditional BSP -- for that see below.
// Note Sync strategies are not compatible with notification-only algorithms (e.g. a pure-message-passing strategy).
func ConvergeSync[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	SendInitialMail(alg, g)
	if g.Options.OracleCompareSync {
		CompareToOracle(alg, g, true, true, false, false)
	}
	iteration := 1
	vertexUpdates := make([]int, g.NumThreads)

	for ; ; iteration++ {
		activity := g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) (tActivity int) {
			var mail M
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				vertex, mailbox := gt.VertexAndMailbox(i)
				active := atomic.SwapInt32(&mailbox.Activity, 0) == 1
				if active {
					gt.NotificationQueue.Accept() // must exist, should discard
					mail = alg.MailRetrieve(&mailbox.Inbox, vertex)
					sent := alg.OnUpdateVertex(g, gt, vertex, Notification[N]{Target: (i | threadOffset)}, mail)
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

// A sync variant that only only consumes information that was generated from the **previous** iteration.
// Does not consider any information generated from the **current** iteration.
// This is a proper emulation of Bulk Synchronous Parallel.
func ConvergeSyncPrevOnly[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	SendInitialMail(alg, g)
	if g.Options.OracleCompareSync {
		CompareToOracle(alg, g, true, true, false, false)
	}
	iteration := 1
	vertexUpdates := make([]int, g.NumThreads)
	numVertices := g.NodeVertexCount()
	mailLast := make([]M, numVertices)
	frontier := make([]bool, numVertices)

	for ; ; iteration++ {
		g.NodeParallelFor(func(ordinalStart, _ uint32, gt *GraphThread[V, E, M, N]) int {
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				vertex, mailbox := gt.VertexAndMailbox(i)
				active := atomic.SwapInt32(&mailbox.Activity, 0) == 1
				if active {
					gt.NotificationQueue.Accept() // must exist, should discard
					mailLast[ordinalStart+i] = alg.MailRetrieve(&mailbox.Inbox, vertex)
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
					sent := alg.OnUpdateVertex(g, gt, gt.Vertex(i), Notification[N]{Target: (i | threadOffset)}, mailLast[idx])
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
