package graph

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Sends message(s) that will start the algorithm.
func SendInitialMessages[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	if aBVM, ok := any(alg).(AlgorithmBaseVertexMessage[V, E, M, N]); ok {
		now := g.AlgTimer.Elapsed()
		// First, set the message to the algorithm defined base value.
		g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) int {
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				vertex, vtm := gt.VertexAndMessages(i)
				vtm.Inbox = aBVM.BaseVertexMessage(vertex, (threadOffset | i), gt.VertexRawID(i))
			}
			return 0
		})
		log.Trace().Msg(", base_messages, " + utils.F("%0.3f", (g.AlgTimer.Elapsed()-now).Seconds()*1000))
	}

	now := g.AlgTimer.Elapsed()
	if g.InitMessages == nil {
		if aIAM, ok := any(alg).(AlgorithmInitAllMessage[V, E, M, N]); ok {
			// Target all vertices: send the algorithm defined initial visit value as a message.
			g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) int {
				sent := uint64(0)
				for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
					vertex, vtm := gt.VertexAndMessages(i)
					rawId := gt.VertexRawID(i)
					vidx := (threadOffset | i)

					msg := aIAM.InitAllMessage(vertex, vidx, rawId)

					if newInfo := alg.MessageMerge(msg, vidx, &vtm.Inbox); newInfo {
						activity := atomic.LoadInt32(&vtm.Activity)
						msg = alg.MessageRetrieve(&vtm.Inbox, vertex)
						sent += alg.OnUpdateVertex(g, vertex, Notification[N]{Target: vidx, Activity: activity}, msg)
					}
				}
				gt.MsgSend += sent
				return 0
			})
		} else {
			log.Warn().Msg("WARNING: No initial messages defined for algorithm? InitMessages is nil and algorithm does not implement InitAllMessage.")
		}
	} else {
		// Target specific vertices: send the algorithm defined initial visit value as a message.
		for vRawId, msg := range g.InitMessages {
			vidx, vertex := g.NodeVertexFromRaw(vRawId)
			if vertex == nil {
				log.Warn().Msg("WARNING: Target source init vertex not found: " + utils.V(vRawId))
				continue
			}
			vtm, tidx := g.NodeVertexMessages(vidx)

			if newInfo := alg.MessageMerge(msg, vidx, &vtm.Inbox); newInfo {
				msg = alg.MessageRetrieve(&vtm.Inbox, vertex)
				activity := atomic.LoadInt32(&vtm.Activity)
				sent := alg.OnUpdateVertex(g, vertex, Notification[N]{Target: vidx, Activity: activity}, msg)
				g.GraphThreads[tidx].MsgSend += sent
			}
		}
	}
	log.Trace().Msg(", init_messages, " + utils.F("%.3f", (g.AlgTimer.Elapsed()-now).Seconds()*1000))
}

// Will pull a bundle of messages targeting this thread, and then process them all.
// Will check for termination only if the bool is set.
func ProcessMessages[V VPI[V], E EPI[E], M MVI[M], N any](alg Algorithm[V, E, M, N], g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], exitCheck bool) (done bool, algCount int) {

	// First check for any back-pressure messages from the last attempt. This are first in FIFO.
	if gt.NotificationBuff.Len() != 0 {
		for ; algCount < MSG_MAX; algCount++ {
			if notif, ok := gt.NotificationBuff.TryPopFront(); !ok {
				break
			} else {
				gt.NotificationBuff.UpdatePopFront()
				gt.Notifications[algCount] = notif
			}
		}
	}

	// If we still have room (no back-pressure), then pull directly from the queue.
	for ; algCount < MSG_MAX; algCount++ {
		if notif, ok := gt.NotificationQueue.Accept(); !ok {
			break
		} else {
			gt.Notifications[algCount] = notif
		}
	}

	// If we have too many messages in the queue, drain them all: push them back into the back-pressure queue.
	if gt.NotificationQueue.DeqCheckRange() > (gt.NotificationQueue.DeqCap())/2 { // TODO: good ratio?
		if g.warnBackPressure == 0 && atomic.CompareAndSwapUint64(&g.warnBackPressure, 0, 1) {
			log.Warn().Msg("WARNING: Detected large pressure on notification queue. Consider increasing flag \"-m\" capacity.")
			log.Warn().Msg("Will attempt to prevent blocking with a second buffer.")
		}
		for { // TODO: we should probably call this if we get blocked while trying to send messages.
			if notif, ok := gt.NotificationQueue.Accept(); !ok {
				break
			} else {
				if ok := gt.NotificationBuff.FastPushBack(notif); !ok {
					gt.NotificationBuff.SlowPushBack(notif)
				}
			}
		}
	}

	sent := uint64(0)
	// Process all messages that we pulled.
	for i := 0; i < algCount; i++ {
		vertex, vtm := gt.VertexAndMessages(gt.Notifications[i].Target)
		gt.Notifications[i].Activity = atomic.AddInt32(&(vtm.Activity), -1)
		msg := alg.MessageRetrieve(&(vtm.Inbox), vertex)
		sent += alg.OnUpdateVertex(g, vertex, gt.Notifications[i], msg)
	}

	if algCount != 0 { // Update send and receive counts.
		gt.MsgSend += sent
		gt.MsgRecv += uint64(algCount)
	} else if exitCheck {
		if g.CheckTermination(gt.Tidx) {
			return true, algCount
		}
	}
	return false, algCount
}

// A thread that will process messages until it is done.
func ConvergeAsyncThread[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], tidx uint32, wg *sync.WaitGroup) {
	runtime.LockOSThread()
	gt := &g.GraphThreads[tidx]
	gt.Status = APPLY_MSG
	algCount := 0
	algNoCountTimes := 0
	for completed := false; !completed; {
		completed, algCount = ProcessMessages[V, E, M, N](alg, g, gt, true)

		if algCount == 0 { // Minor back off if we didn't get, and keep getting, no messages.
			algNoCountTimes++
			if algNoCountTimes%100 == 0 {
				gt.Status = BACKOFF_ALG
				utils.BackOff(algNoCountTimes / 100)
				gt.Status = APPLY_MSG
			}
		} else {
			algNoCountTimes = 0
		}
	}
	gt.Status = DONE
	wg.Done()
	runtime.UnlockOSThread()
}

// Static focused variant of async convergence.
func ConvergeAsync[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], wg *sync.WaitGroup) {
	wg.Add(int(g.NumThreads))
	stopTimers := false

	if g.Options.DebugLevel >= 3 { // For checking termination status.
		go g.PrintTerminationStatus(&stopTimers)
	}

	// Send initial visit message(s)
	SendInitialMessages(alg, g)

	// Launch threads.
	for t := uint32(0); t < g.NumThreads; t++ {
		go ConvergeAsyncThread(alg, g, t, wg)
	}

	wg.Wait() // Wait for alg termination.
	stopTimers = true
	g.EnsureCompleteness()
}
