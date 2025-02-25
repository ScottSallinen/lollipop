package graph

import (
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// Prints the current dynamic event rates during processing.
func (g *Graph[V, E, M, N]) PrintEventRate(exit *bool) {
	timeLast := g.Watch.Elapsed().Seconds()

	if g.Options.PollingRate == 0 {
		g.Options.PollingRate = 1000
	}
	pollRate := time.Duration(g.Options.PollingRate) * time.Millisecond

	time.Sleep(pollRate)

	allEventCountLast := 0
	targetRate := float64(g.Options.TargetRate)
	if targetRate == 0 {
		targetRate = 1e16
	}
	statusArr := make([]GraphThreadStatus, g.NumThreads)

	for !(*exit) {
		timeNow := g.Watch.Elapsed().Seconds()
		if timeNow == timeLast { // Watch paused, likely comparing to oracle (so no events)
			time.Sleep(pollRate)
			continue
		}
		allAdds := 0
		allDels := 0
		for t := 0; t < int(g.NumThreads); t++ { // No need to lock, as we do not care for a consistent view, only approximate
			allAdds += int(g.GraphThreads[t].NumOutAdds)
			allDels += int(g.GraphThreads[t].NumOutDels)
		}
		allEventCount := allAdds + allDels
		allRate := float64(allEventCount) / timeNow
		currentRate := float64(allEventCount-allEventCountLast) / (timeNow - timeLast)
		str := utils.F("%.2f", timeNow) + "s EdgeAdds " + utils.V(allAdds)

		if allDels > 0 {
			str += " EdgeDels " + utils.V(allDels) + " AllEvents " + utils.V(allEventCount)
		}
		str += " TotalRate " + utils.V(int64(allRate)) + " CurrentRate " + utils.V(int64(currentRate))
		if targetRate != 1e16 {
			str += " AllRateAchieved " + utils.F("%.3f", (allRate*100.0)/float64(targetRate))
		}
		log.Info().Msg(str)

		// Count how many threads are in each status
		for i := 0; i < int(g.NumThreads); i++ {
			statusArr[i] = g.GraphThreads[i].Status
		}
		statusCounts := make([]int, DONE+1)
		for _, status := range statusArr {
			statusCounts[status]++
		}
		topIndexes := utils.SortGiveIndexesLargestFirst(statusCounts)

		// For each status, print the number of threads in that status
		line := ""
		for _, i := range topIndexes {
			count := statusCounts[i]
			if count > 0 {
				line += GraphThreadStatus(i).String() + ": " + utils.V(count) + " "
			}
		}
		log.Debug().Msg(line)

		if g.Options.DebugLevel >= 2 {
			loopTimes := g.GraphThreads[0].LoopTimes
			log.Trace().Msg(", recv_cmd, " + utils.F("%0.2f", loopTimes[RECV_CMD].Seconds()*1000) +
				", remit, " + utils.F("%0.2f", loopTimes[REMIT].Seconds()*1000) +
				", recv_top, " + utils.F("%0.2f", loopTimes[RECV_TOP].Seconds()*1000) +
				", apply_top, " + utils.F("%0.2f", loopTimes[APPLY_TOP].Seconds()*1000) +
				", apply_msg, " + utils.F("%0.2f", loopTimes[APPLY_MSG].Seconds()*1000) +
				", backoff_top, " + utils.F("%0.2f", loopTimes[BACKOFF_TOP].Seconds()*1000) +
				", backoff_msg, " + utils.F("%0.2f", loopTimes[BACKOFF_ALG].Seconds()*1000))
		}

		if g.Options.DebugLevel >= 3 || g.Options.Profile {
			utils.MemoryStats()
		}

		timeLast = timeNow
		allEventCountLast = allEventCount
		time.Sleep(pollRate)
	}
}

func (gt *GraphThread[V, E, M, N]) checkCommandsDynamic(blockTop, eventSync, blockAlgIfEvents, epoch *bool) {
	switch <-gt.Command {
	case BLOCK_ALL:
		gt.Response <- ACK
		resp := <-gt.Command // BLOCK and wait for resume
		if resp != RESUME {
			log.Panic().Msg("Expected to resume after blocked")
		}
	case BLOCK_EVENTS:
		*blockTop = true
		gt.Response <- ACK
	case BLOCK_EVENTS_ASYNC:
		*blockTop = true
		// No ack needed.
	case RESUME:
		*blockTop = false
		// No ack needed.
	case BLOCK_ALG_IF_EVENTS:
		*blockAlgIfEvents = true
		// Init command, no ack needed.
	case EVENT_SYNC:
		*eventSync = true
		// We will ack later, after we have processed events.
	case EPOCH:
		*epoch = true
		// Ack after complete.
	}
}

// The main loop (e.g., state machine) for a single thread.
func ConvergeDynamicThread[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], tidx uint32, wg *sync.WaitGroup, doneEvents chan struct{}, doneRemit chan struct{}) {
	runtime.LockOSThread()

	completed := false   // True indicates the algorithm is done (termination detected).
	inputClosed := false // True indicates events (input emitted) are done.
	remitClosed := false // True indicates no more events will be remitted.
	//remitFailed := false // True indicates a remit failed.
	blockTop := false
	eventSync := false
	blockAlgIfEvents := false
	epoch := false
	wantsQueryNow := false
	wantsAsyncQueryNow := false
	prevMismatch := false
	undirected := g.Options.Undirected
	makeTimeseries := (g.Options.LogTimeseries)
	_, checkSuperStep := any(alg).(AlgorithmOnSuperStepConverged[V, E, M, N])

	gt := &g.GraphThreads[tidx]
	insDelOnExpire := g.Options.InsertDeleteOnExpire

	pullUpToBase := uint64(BASE_SIZE) * 2
	algMessageCount := uint64(0)
	algMessageForHeight := uint64(0)

	remitCount := uint64(0)
	remitTotalCount := uint64(0)

	largest := uint64(0)
	currLargest := uint64(0)

	eventsHeight := uint64(0)
	eventsApplyCountNow := uint64(0)
	eventsTotalCount := uint64(0)
	eventsFlushedUpTo := uint64(0)
	algFail := 0
	topFail := 0

	loopsFail := 0
	loopsThrough := 0

	var onInEdgeAddFunc func(*Graph[V, E, M, N], *GraphThread[V, E, M, N], *Vertex[V, E], *V, uint32, uint32, *InputEvent[E])
	if aIN, ok := any(alg).(AlgorithmOnInEdgeAdd[V, E, M, N]); ok {
		onInEdgeAddFunc = aIN.OnInEdgeAdd
	}

	timeStates := func() {}
	if (g.Options.DebugLevel >= 2) && tidx == 0 {
		var prev, curr time.Time
		gt.LoopTimes = make([]time.Duration, DONE)
		timeStates = func() {
			curr = time.Now()
			gt.LoopTimes[gt.Status] += curr.Sub(prev)
			prev = curr
		}
		prev = time.Now()
	}

	for !completed {
		if len(gt.Command) > 0 {
			gt.Status = RECV_CMD
			gt.checkCommandsDynamic(&blockTop, &eventSync, &blockAlgIfEvents, &epoch)
		}
		timeStates()

		processAlg := inputClosed || !(EVENTS_FIRST || blockAlgIfEvents)
		algMessageHeightChanged := false
		if processAlg && !prevMismatch {
			// Receive algorithmic messages; before topology events, to avoid messages that could be over topology that we have not yet processed.
			if messageCountNew := ReceiveMessages(alg, g, gt, algMessageCount); messageCountNew > algMessageCount {
				algMessageCount = messageCountNew
				algMessageHeightChanged = true
			}
		}
		eventsHeight = gt.FromRemitQueue.Height()
		if algMessageHeightChanged {
			algMessageForHeight = eventsHeight
		}

		if !epoch && !remitClosed && !blockTop && !wantsQueryNow && !wantsAsyncQueryNow {
			gt.Status = REMIT
			remitClosed, remitCount, currLargest, _, wantsQueryNow = checkToRemit(alg, g, gt, onInEdgeAddFunc, eventsFlushedUpTo)
			largest = utils.Max(largest, currLargest)
			gt.EventActions += remitCount
			remitTotalCount += remitCount
			if remitClosed {
				//log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] FromEmitEvents closed")
				gt.FromEmitQueue.End()
				doneRemit <- struct{}{}
			}

			if wantsQueryNow { // Did we JUST receive a request for query?
				// First, this thread needs to acknowledge we saw the query.
				gt.Response <- ACK
				<-gt.Command // BLOCK and wait for resume (So all threads confirmed they have seen the query, and all threads must have emplaced their remits to us).

				if g.Options.NoConvergeForQuery {
					wantsQueryNow = false
					wantsAsyncQueryNow = true
				}
			}
			timeStates()
		} else {
			remitCount = 0
		}

		// The main check for updates from events. This occurs with priority over algorithmic messages.
		processTop := !inputClosed && !blockTop
		topFailed := false
		if processTop {
			gt.Status = RECV_TOP

			if remitClosed && remitTotalCount == eventsTotalCount && gt.FromEmitQueue.IsClosed() {
				inputClosed = true
				gt.TopologyEventBuff = nil
				gt.VertexPendingBuff = nil
				doneEvents <- struct{}{}
				gt.FromRemitQueue.End()
			}

			prevMismatch = false
			for eventsApplyCountNow = 0; remitTotalCount > eventsTotalCount; eventsApplyCountNow++ {
				if remitEvent, ok := gt.FromRemitQueue.Accept(); !ok {
					if eventsTotalCount < algMessageForHeight {
						processAlg = false
						prevMismatch = true
						loopsFail++
					}
					//log.Info().Msg("T[" + utils.F("%02d", tidx) + "] events Gap In Order")
					break
				} else {
					eventsTotalCount++
					targetIndex := remitEvent.Order - eventsFlushedUpTo
					gt.TopologyEventBuff[targetIndex].Edge = remitEvent.Edge // They told us the edge.
				}
			}
			timeStates()

			// Apply any events that we have pulled.
			if eventsApplyCountNow != 0 {
				gt.Status = APPLY_TOP
				addEvents, delEvents := EnactTopologyEvents[EP](alg, g, gt, eventsApplyCountNow, insDelOnExpire, undirected)
				gt.NumOutAdds += addEvents
				gt.NumOutDels += delEvents
				gt.NumEdges += addEvents - delEvents

				//log.Info().Msg("T[" + utils.F("%02d", tidx) + "] eventsApplyCountNow " + utils.V(eventsApplyCountNow) + " remitCount " + utils.V(remitCount))

				// shuffle down the event buffer
				copy(gt.TopologyEventBuff, gt.TopologyEventBuff[eventsApplyCountNow:(largest-eventsFlushedUpTo)+1])
				eventsFlushedUpTo += eventsApplyCountNow
				gt.EventActions += eventsApplyCountNow

				timeStates()

				// If we filled bundle then we loop back to ingest more events, rather than process algorithm messages, as the thread is behind.
				if (eventsApplyCountNow >= uint64(len(gt.TopologyEventBuff)/2)) || eventSync {
					if uint64(gt.NumEdges)/(64) > pullUpToBase {
						pullUpToBase = pullUpToBase * 2
						if len(gt.TopologyEventBuff) < int(pullUpToBase) {
							gt.TopologyEventBuff = append(gt.TopologyEventBuff, make([]RawEdgeEvent[E], pullUpToBase)...)
							log.Info().Msg("T[" + utils.F("%02d", tidx) + "] increasing buffer size to " + utils.V(len(gt.TopologyEventBuff)))
						}
					}
					continue
				} else if makeTimeseries {
					continue
				}
			} else if eventSync && remitCount == 0 {
				// There is a request for syncing topology (blocking), acknowledge here as we have no more events to process.
				eventSync = false
				gt.Response <- ACK
			} else if remitCount == 0 && !epoch {
				topFailed = true
			}
		}

		if wantsAsyncQueryNow { // This type of query is more of a "sample", since it does not wait for convergence.
			// We applied available events, so we are good to report continuation.
			gt.Response <- ACK
			wantsAsyncQueryNow = false
			blockTop = true // We will block further topology until we receive the resume command.
			// TODO: Check for g.Options.AllowAsyncVertexProps , need to adjust for this.
		}

		algFailed := false
		if processAlg {
			if processTop {
				loopsThrough++
			}
			// Process algorithm messages. Check for algorithm termination if needed.
			gt.Status = APPLY_MSG
			checkTerm := (inputClosed && remitClosed) || // all events are done, or
				(blockTop && checkSuperStep) || // events are currently blocked and we may super-step, (NoConvergeForQuery blocks top, would not have super-steps, but might complete)
				(epoch && eventsApplyCountNow == 0) || // no more events in this epoch (assuming the remitter does not produce new events after it starts an epoch)
				(wantsQueryNow && eventsApplyCountNow == 0) // no more events, and we are ready to query
			completed = ProcessMessages(alg, g, gt, algMessageCount, checkTerm)
			if !completed && algMessageCount == 0 {
				algFailed = true
			}
			algMessageCount = 0

			if completed && checkSuperStep {
				completed = AwaitSuperStepConvergence(alg, g, tidx)
			}
			if completed && wantsQueryNow {
				gt.Status = DONE
				gt.Response <- ACK // This is a second ack, the first was when we saw the request for query.

				resp := <-gt.Command // BLOCK and wait for resume
				if resp != RESUME {
					log.Panic().Msg("Expected to resume after blocked")
				}
				wantsQueryNow = false
				completed = false
				topFail, algFail = 0, 0
			}
			if completed && epoch {
				//log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] completed epoch")
				gt.Status = DONE
				gt.Response <- ACK

				resp := <-gt.Command // BLOCK and wait for resume
				if resp != RESUME {
					log.Panic().Msg("Expected to resume after blocked")
				}
				epoch = false
				completed = false
				topFail, algFail = 0, 0
			}
			if completed && blockTop {
				completed = false
				log.Panic().Msg("Algorithm terminated when events are blocked")
			}
			timeStates()
		}

		// Backoff only when there are no topological events OR algorithm messages
		if (!processTop || topFailed) && (!processAlg || algFailed) {
			if topFailed {
				gt.Status = BACKOFF_TOP
				utils.BackOff(topFail)
				timeStates()
				topFail++
			} else {
				topFail = 0
			}
			if algFailed {
				if algFail%10 == 0 {
					gt.Status = BACKOFF_ALG
					utils.BackOff(algFail / 10)
					timeStates()
				}
				algFail++
			} else {
				algFail = 0
			}
		} else {
			topFail, algFail = 0, 0
		}
	}
	log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] loopsThrough " + utils.V(loopsThrough) + " loopsFail " + utils.V(loopsFail))
	gt.Status = DONE
	wg.Done()
	runtime.UnlockOSThread()
}

// Dynamic focused variant of async convergence.
func ConvergeDynamic[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], inputWg *sync.WaitGroup) {
	wg := new(sync.WaitGroup)
	THREADS := g.NumThreads
	wg.Add(int(THREADS))
	stopTimers := false

	if g.Options.DebugLevel >= 3 { // For checking termination status.
		go g.PrintTerminationStatus(&stopTimers)
	}

	doneEvents := make(chan struct{}, THREADS)
	doneRemit := make(chan struct{}, THREADS)

	for t := 0; t < int(THREADS); t++ {
		go ConvergeDynamicThread[EP](alg, g, uint32(t), wg, doneEvents, doneRemit)
	}

	go g.PrintEventRate(&stopTimers)

	// Wait for stream to be concluded.
	inputWg.Wait()
	//debug.FreeOSMemory()

	// Wait for all threads to be done remitting events.
	for i := 0; i < int(THREADS); i++ {
		<-doneRemit
	}
	log.Debug().Msg("Seen acknowledge of done remits after (ms) " + utils.V(g.Watch.Elapsed().Milliseconds()))
	for t := 0; t < int(THREADS); t++ {
		g.GraphThreads[t].FromRemitQueue.Close()
	}

	// Wait for all events to be consumed.
	for i := 0; i < int(THREADS); i++ {
		<-doneEvents
	}

	allAdds := 0
	allDels := 0
	for t := 0; t < int(g.NumThreads); t++ {
		allAdds += int(g.GraphThreads[t].NumOutAdds)
		allDels += int(g.GraphThreads[t].NumOutDels)
	}

	elapsed := g.Watch.Elapsed()
	str := "All events consumed in (ms): " + utils.V(elapsed.Milliseconds()) + " EdgeAdds " + utils.V(allAdds)
	if allDels != 0 {
		str += " EdgeDels " + utils.V(allDels) + " AllEvents " + utils.V(allAdds+allDels)
	}
	log.Info().Msg(str)
	log.Trace().Msg(", consumed, " + utils.F("%.3f", elapsed.Seconds()*1000))

	wg.Wait() // Wait for alg termination.
	stopTimers = true
	g.EnsureCompleteness()
}
