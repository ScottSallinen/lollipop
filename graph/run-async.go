package graph

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Sends initial data that will start the algorithm.
func SendInitialMail[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N]) {
	if aBVM, ok := any(alg).(AlgorithmBaseVertexMailbox[V, E, M, N]); ok {
		now := g.AlgTimer.Elapsed()
		// First, set the mailbox to the algorithm defined base value.
		g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) int {
			for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
				vertex, mailbox := gt.VertexAndMailbox(i)
				prop := gt.VertexProperty(i)
				mailbox.Inbox = aBVM.BaseVertexMailbox(vertex, prop, (threadOffset | i), gt.VertexStructure(i))
			}
			return 0
		})
		log.Trace().Msg(", base_mailbox, " + utils.F("%0.3f", (g.AlgTimer.Elapsed()-now).Seconds()*1000))
	}

	now := g.AlgTimer.Elapsed()
	if g.InitMails == nil {
		if aIAM, ok := any(alg).(AlgorithmInitAllMail[V, E, M, N]); ok { // Target all vertices: send the algorithm defined initial value as mail.
			g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) int {
				sent := uint64(0)
				for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
					vertex, mailbox := gt.VertexAndMailbox(i)
					rawId := gt.VertexRawID(i)
					vidx := (threadOffset | i)
					prop := gt.VertexProperty(i)
					mail := aIAM.InitAllMail(vertex, prop, vidx, rawId)

					if newInfo := alg.MailMerge(mail, vidx, &mailbox.Inbox); newInfo {
						activity := atomic.LoadInt32(&mailbox.Activity)
						mail = alg.MailRetrieve(&mailbox.Inbox, vertex, prop)
						sent += alg.OnUpdateVertex(g, gt, vertex, prop, Notification[N]{Target: vidx, Activity: activity}, mail)
					}
				}
				gt.MsgSend += sent
				return 0
			})
		} else if g.InitNotes == nil {
			if aIAM, ok := any(alg).(AlgorithmInitAllNote[V, E, M, N]); ok { // Target all vertices: send the algorithm defined initial notification.
				g.NodeParallelFor(func(_, threadOffset uint32, gt *GraphThread[V, E, M, N]) int {
					sent := uint64(0)
					for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
						vertex, mailbox := gt.VertexAndMailbox(i)
						rawId := gt.VertexRawID(i)
						vidx := (threadOffset | i)
						prop := gt.VertexProperty(i)

						note := aIAM.InitAllNote(vertex, prop, vidx, rawId)

						n := Notification[N]{Target: vidx, Note: note}
						sent += g.EnsureSend(g.ActiveNotification(vidx, n, mailbox, uint32(gt.Tidx)))
					}
					gt.MsgSend += sent
					return 0
				})
			} else {
				log.Warn().Msg("WARNING: No initial data defined for algorithm? InitMails/Notes are nil and algorithm does not implement InitAllMail/Note.")
			}
		} else {
			// Target specific vertices: send the algorithm defined initial notification.
			for vRawId, note := range g.InitNotes {
				vidx, vertex := g.NodeVertexFromRaw(vRawId)
				if vertex == nil {
					log.Warn().Msg("WARNING: Target source init vertex not found: " + utils.V(vRawId))
					continue
				}
				n := Notification[N]{Target: vidx, Note: note}
				mailbox, tidx := g.NodeVertexMailbox(vidx)
				sent := g.EnsureSend(g.ActiveNotification(vidx, n, mailbox, tidx))
				g.GraphThreads[tidx].MsgSend += sent
			}
		}
	} else { // Target specific vertices: send the algorithm defined initial value as mail.
		for vRawId, mail := range g.InitMails {
			vidx, vertex := g.NodeVertexFromRaw(vRawId)
			if vertex == nil {
				log.Warn().Msg("WARNING: Target source init vertex not found: " + utils.V(vRawId))
				continue
			}
			mailbox, tidx := g.NodeVertexMailbox(vidx)

			if newInfo := alg.MailMerge(mail, vidx, &mailbox.Inbox); newInfo {
				prop := g.NodeVertexProperty(vidx)
				mail = alg.MailRetrieve(&mailbox.Inbox, vertex, prop)
				activity := atomic.LoadInt32(&mailbox.Activity)
				sent := alg.OnUpdateVertex(g, &g.GraphThreads[tidx], vertex, prop, Notification[N]{Target: vidx, Activity: activity}, mail)
				g.GraphThreads[tidx].MsgSend += sent
			}
		}
	}
	log.Trace().Msg(", init_send, " + utils.F("%.3f", (g.AlgTimer.Elapsed()-now).Seconds()*1000))
}

// Will pull a bundle of notifications targeting this thread.
// A notification represents a vertex is 'active' as it has work to do (e.g. has mail in its inbox, or the notification itself is important).
// We define a message as a notification that was genuinely sent and is thus in the queue (e.g. it was not discarded due to non-uniqueness).
func ReceiveMessages[V VPI[V], E EPI[E], M MVI[M], N any](alg Algorithm[V, E, M, N], g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], algCount uint64) (newAlgCount uint64) {
	// First check for any back-pressure from the last attempt. This are first in FIFO.
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

	// If we still have room (no more back-pressure), then pull directly from the queue.
	for ; algCount < MSG_MAX; algCount++ {
		if notif, ok := gt.NotificationQueue.Accept(); !ok {
			break
		} else {
			gt.Notifications[algCount] = notif
		}
	}

	// If we have too much in the queue, drain it all: push them back into the back-pressure queue.
	if gt.NotificationQueue.DeqCheckRange() > (gt.NotificationQueue.DeqCap())/2 { // TODO: good ratio?
		if g.warnBackPressure == 0 && atomic.CompareAndSwapUint64(&g.warnBackPressure, 0, 1) {
			log.Warn().Msg("WARNING: Detected large pressure on queue. Consider increasing flag \"-m\" capacity.")
			log.Warn().Msg("Will attempt to prevent blocking with a second buffer.")
		}
		for { // TODO: we should probably call this if we get blocked while trying to send.
			if notif, ok := gt.NotificationQueue.Accept(); !ok {
				break
			} else {
				if ok := gt.NotificationBuff.FastPushBack(notif); !ok {
					gt.NotificationBuff.SlowPushBack(notif)
				}
			}
		}
	}
	return algCount
}

// Will process the messages in the queue.
// Will check for termination only if the bool is set.
func ProcessMessages[V VPI[V], E EPI[E], M MVI[M], N any](alg Algorithm[V, E, M, N], g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], algCount uint64, exitCheck bool) (done bool) {
	sent := uint64(0)
	// Process all that we pulled.
	for i := uint64(0); i < algCount; i++ {
		vertex, mailbox := gt.VertexAndMailbox(gt.Notifications[i].Target)
		gt.Notifications[i].Activity = atomic.AddInt32(&(mailbox.Activity), -1)
		prop := gt.VertexProperty(gt.Notifications[i].Target)
		mail := alg.MailRetrieve(&(mailbox.Inbox), vertex, prop)
		sent += alg.OnUpdateVertex(g, gt, vertex, prop, gt.Notifications[i], mail)
	}

	if algCount != 0 { // Update send and receive counts.
		gt.MsgSend += sent
		gt.MsgRecv += algCount
	} else if exitCheck {
		if g.CheckTermination(gt.Tidx) {
			return true
		}
	}
	return false
}

func (gt *GraphThread[V, E, M, N]) checkCommandsAsync(epoch *bool) {
	switch <-gt.Command {
	case BLOCK_ALL:
		gt.Response <- ACK
		resp := <-gt.Command // BLOCK and wait for resume
		if resp != RESUME {
			log.Panic().Msg("Expected to resume after blocked")
		}
	case BLOCK_TOP:
		log.Panic().Msg("There's no topology changes to block")
	case RESUME:
		// No ack needed.
		break
	case BLOCK_ALG_IF_TOP:
		log.Panic().Msg("There's no topology changes")
	case TOP_SYNC:
		log.Panic().Msg("There's no topology changes")
	case EPOCH:
		*epoch = true
		// Ack after complete.
	}
}

// A thread that will process messages until it is done.
func ConvergeAsyncThread[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], tidx uint32, wg *sync.WaitGroup) {
	runtime.LockOSThread()
	gt := &g.GraphThreads[tidx]
	gt.Status = APPLY_MSG
	_, checkSuperStep := any(alg).(AlgorithmOnSuperStepConverged[V, E, M, N])
	algCount := uint64(0)
	algNoCountTimes := 0
	epoch := false

	for completed := false; !completed; {
		if len(gt.Command) > 0 {
			gt.Status = RECV_CMD
			gt.checkCommandsAsync(&epoch)
			gt.Status = APPLY_MSG
		}

		algCount = ReceiveMessages[V, E, M, N](alg, g, gt, algCount)
		completed = ProcessMessages[V, E, M, N](alg, g, gt, algCount, true)
		if !completed && algCount == 0 { // Minor back off if we didn't get, and keep getting, no messages.
			algNoCountTimes++
			if algNoCountTimes%100 == 0 {
				gt.Status = BACKOFF_ALG
				utils.BackOff(algNoCountTimes / 100)
				gt.Status = APPLY_MSG
			}
		} else {
			algNoCountTimes = 0
		}
		algCount = 0

		if completed && checkSuperStep {
			completed = AwaitSuperStepConvergence[V, E, M, N](alg, g, tidx)
		}
		if completed && epoch {
			gt.Status = DONE
			gt.Response <- ACK
			resp := <-gt.Command // BLOCK and wait for resume
			if resp != RESUME {
				log.Panic().Msg("Expected to resume after blocked")
			}
			epoch = false
			completed = false
			algNoCountTimes = 0
			gt.Status = APPLY_MSG
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

	// Send initial data to start the algorithm.
	SendInitialMail(alg, g)

	// Launch threads.
	for t := uint32(0); t < g.NumThreads; t++ {
		go ConvergeAsyncThread(alg, g, t, wg)
	}

	wg.Wait() // Wait for algorithm termination.
	stopTimers = true
	g.EnsureCompleteness()
}
