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
			log.Trace().Msg(", recv_cmd, " + utils.F("%0.2f", loopTimes[0].Seconds()*1000) +
				", remit, " + utils.F("%0.2f", loopTimes[1].Seconds()*1000) +
				", recv_top, " + utils.F("%0.2f", loopTimes[2].Seconds()*1000) +
				", apply_top, " + utils.F("%0.2f", loopTimes[3].Seconds()*1000) +
				", apply_msg, " + utils.F("%0.2f", loopTimes[4].Seconds()*1000) +
				", backoff_top, " + utils.F("%0.2f", loopTimes[5].Seconds()*1000) +
				", backoff_msg, " + utils.F("%0.2f", loopTimes[6].Seconds()*1000))
		}

		if g.Options.DebugLevel >= 3 || g.Options.Profile {
			utils.MemoryStats()
		}

		timeLast = timeNow
		allEventCountLast = allEventCount
		time.Sleep(pollRate)
	}
}

func (gt *GraphThread[V, E, M, N]) checkCommandsDynamic(blockTop, topSync, blockAlgIfTop, epoch *bool) {
	switch <-gt.Command {
	case BLOCK_ALL:
		gt.Response <- ACK
		resp := <-gt.Command // BLOCK and wait for resume
		if resp != RESUME {
			log.Panic().Msg("Expected to resume after blocked")
		}
	case BLOCK_TOP:
		*blockTop = true
		gt.Response <- ACK
	case BLOCK_TOP_ASYNC:
		*blockTop = true
		// No ack needed.
	case RESUME:
		*blockTop = false
		// No ack needed.
	case BLOCK_ALG_IF_TOP:
		*blockAlgIfTop = true
		// Init command, no ack needed.
	case TOP_SYNC:
		*topSync = true
		// We will ack later, after we have processed events.
	case EPOCH:
		*epoch = true
		// Ack after complete.
	}
}

// The main loop (e.g., state machine) for a single thread.
func ConvergeDynamicThread[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], tidx uint32, wg *sync.WaitGroup, doneEvents chan struct{}, doneRemit chan struct{}) {
	completed := false   // True indicates the algorithm is done (termination detected).
	strucClosed := false // True indicates the TopologyEvents channel is closed
	remitClosed := false // True indicates the Remitter StructureChanges channel is closed
	gt := &g.GraphThreads[tidx]

	_, checkSuperStep := any(alg).(AlgorithmOnSuperStepConverged[V, E, M, N])
	makeTimeseries := (g.Options.LogTimeseries)
	insDelOnExpire := g.Options.InsertDeleteOnExpire
	pullUpToBase := uint64(BASE_SIZE)
	blockTop := false
	topSync := false
	blockAlgIfTop := false
	epoch := false
	var ok bool
	topCount := uint64(0)
	algCount := uint64(0)
	remitCount := uint64(0)
	algFail := 0
	topFail := 0
	timeStates := g.Options.DebugLevel >= 2
	if timeStates {
		gt.LoopTimes = make([]time.Duration, DONE)
	}
	runtime.LockOSThread()
	var onInEdgeAddFunc func(*Graph[V, E, M, N], *GraphThread[V, E, M, N], *Vertex[V, E], *V, uint32, uint32, *TopologyEvent[E])
	if aIN, ok := any(alg).(AlgorithmOnInEdgeAdd[V, E, M, N]); ok {
		onInEdgeAddFunc = aIN.OnInEdgeAdd
	}

	var prev, curr time.Time

	for !completed {
		if timeStates && tidx == 0 {
			prev = time.Now()
		}
		if len(gt.Command) > 0 {
			gt.Status = RECV_CMD
			gt.checkCommandsDynamic(&blockTop, &topSync, &blockAlgIfTop, &epoch)
		}
		if timeStates && tidx == 0 {
			curr = time.Now()
			gt.LoopTimes[0] += curr.Sub(prev)
			prev = curr
		}

		if !epoch && !remitClosed && !blockTop {
			gt.Status = REMIT
			remitClosed, remitCount = checkToRemit(alg, g, gt, onInEdgeAddFunc)
			gt.EventActions += remitCount
			if remitClosed {
				//log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] FromEmitEvents closed")
				remitCount = 0
				gt.ToRemitQueue.Close()
				gt.FromEmitQueue.End()
				doneRemit <- struct{}{}
			}
			if timeStates && tidx == 0 {
				curr = time.Now()
				gt.LoopTimes[1] += curr.Sub(prev)
				prev = curr
			}
		} else {
			remitCount = 0
		}

		processAlg := strucClosed || !(TOPOLOGY_FIRST || blockAlgIfTop)
		if processAlg {
			// Receive algorithmic messages; before topology events, to avoid messages that could be over topology that we have not yet processed.
			algCount = ReceiveMessages[V, E, M, N](alg, g, gt, algCount)
		}

		// The main check for updates to topology. This occurs with priority over algorithmic messages.
		processTop := !strucClosed && !blockTop
		topFailed := false
		if processTop {
			gt.Status = RECV_TOP
			for topCount = 0; topCount < pullUpToBase; topCount++ {
				gt.TopologyEventBuff[topCount], ok = gt.TopologyQueue.Accept()
				if !ok {
					if gt.TopologyQueue.IsClosed() {
						//log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] TopologyQueue closed")
						strucClosed = true
						gt.TopologyQueue.End()
					}
					break
				}
			}

			if timeStates && tidx == 0 {
				curr = time.Now()
				gt.LoopTimes[2] += curr.Sub(prev)
				prev = curr
			}

			// Apply any topology events that we have pulled.
			if topCount != 0 {
				gt.EventActions += uint64(topCount)
				gt.Status = APPLY_TOP
				addEvents, delEvents := EnactTopologyEvents[EP](alg, g, gt, topCount, insDelOnExpire)
				gt.NumOutAdds += addEvents
				gt.NumOutDels += delEvents
				gt.NumEdges += addEvents - delEvents

				if timeStates && tidx == 0 {
					curr = time.Now()
					gt.LoopTimes[3] += curr.Sub(prev)
					prev = curr
				}

				// If we filled bundle then we loop back to ingest more events, rather than process algorithm messages, as the thread is behind.
				if (topCount == pullUpToBase) || topSync {
					if uint64(gt.NumEdges)/(64) > pullUpToBase {
						pullUpToBase = pullUpToBase * 2
						if len(gt.TopologyEventBuff) < int(pullUpToBase) {
							gt.TopologyEventBuff = make([]InternalTopologyEvent[E], pullUpToBase)
						}
						log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] increasing buffer size to " + utils.V(pullUpToBase))
					}
					if strucClosed {
						doneEvents <- struct{}{}
					}
					continue
				} else if makeTimeseries {
					if strucClosed {
						doneEvents <- struct{}{}
					}
					continue
				}
			} else if topSync && remitCount == 0 {
				// There is a request for syncing topology (blocking), acknowledge here as we have no more topology events to process.
				topSync = false
				gt.Response <- ACK
			} else if remitCount == 0 && !epoch {
				topFailed = true
			}

			if strucClosed {
				gt.TopologyEventBuff = nil
				gt.VertexPendingBuff = nil
				doneEvents <- struct{}{}
			}
		}

		algFailed := false
		if processAlg {
			// Process algorithm messages. Check for algorithm termination if needed.
			gt.Status = APPLY_MSG
			checkTerm := (strucClosed && remitClosed) || // all topology events are done, or
				(blockTop && checkSuperStep) || // topology events are currently blocked and we may super-step, (NoConvergeForQuery blocks top, would not have super-steps, but might complete)
				(epoch && topCount == 0) // no more topology events in this epoch (assuming the remitter does not produce new events after it starts an epoch)
			completed = ProcessMessages[V, E, M, N](alg, g, gt, algCount, checkTerm)
			if !completed && algCount == 0 {
				algFailed = true
			}
			algCount = 0

			if completed && checkSuperStep {
				completed = AwaitSuperStepConvergence[V, E, M, N](alg, g, tidx)
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
				log.Panic().Msg("Algorithm terminated when topology events are blocked")
			}
		}

		// Backoff only when there are no topological events and algorithmic event
		if (!processTop || topFailed) && (!processAlg || algFailed) {
			if topFailed {
				gt.Status = BACKOFF_TOP
				utils.BackOff(topFail)
				if timeStates && tidx == 0 {
					curr = time.Now()
					gt.LoopTimes[5] += curr.Sub(prev)
					prev = curr
				}
				topFail++
			} else {
				topFail = 0
			}
			if algFailed {
				if algFail%10 == 0 {
					gt.Status = BACKOFF_ALG
					utils.BackOff(algFail / 10)
					if timeStates && tidx == 0 {
						curr = time.Now()
						gt.LoopTimes[6] += curr.Sub(prev)
						prev = curr
					}
				}
				algFail++
			} else {
				algFail = 0
			}
		} else {
			topFail, algFail = 0, 0
		}

		if timeStates && tidx == 0 {
			curr = time.Now()
			gt.LoopTimes[4] += curr.Sub(prev)
			prev = curr
		}
	}
	gt.Status = DONE
	//log.Debug().Msg("T[" + utils.F("%02d", tidx) + "] done")
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
