package framework

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

// OnQueueVisitAsync: Async queue applying function; aggregates message values,
// and only injects a visit marker if none exist already.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) OnQueueVisitAsync(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitData MsgType) {
	target := &g.Vertices[didx]
	doSendMessage := frame.MessageAggregator(target, didx, sidx, VisitData)

	// Having multiple visits for the same vertex in the queue at the same time is not ideal but possible. It is
	// difficult to avoid this without some cost. We just rely on the algorithm's MessageAggregator function to report
	// "no work to do" to do it's best to avoid reporting new (and not useful) queue entries.
	// The MessageAggregator can be designed in such a way to ensure uniqueness of a notification;
	// For example, return true only on the transition from zero to non-zero, and not on further increments to a value.
	if doSendMessage {
		select {
		case g.MessageQ[target.ToThreadIdx()] <- graph.Message[MsgType]{Sidx: sidx, Didx: didx, Message: g.EmptyVal}:
		default:
			enforce.ENFORCE(false, "queue error, tidx:", target.ToThreadIdx(), " filled to ", len(g.MessageQ[target.ToThreadIdx()]))
		}
		// must be called by the source vertex's thread
		g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
	}
}

// ConvergeAsync: Static focused variant of async convergence.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) ConvergeAsync(g *graph.Graph[VertexProp, EdgeProp, MsgType], feederWg *sync.WaitGroup) {
	// Note: feederWg not used -- only in the function to match the template ConvergeFunc.
	info("ConvergeAsync")
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	if graph.DEBUG {
		exit := false
		defer func() { exit = true }()
		go PrintTerminationStatus(g, &exit)
	}

	g.TerminateData[VOTES-1] = int64(len(g.Vertices)) // overestimate so we don't accidentally terminate early
	// Send initial visit message(s)
	go func() {
		if !g.SourceInit { // Target all vertices: send the algorithm defined initial visit value as a message.
			acc := make([]uint32, graph.THREADS)
			mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(vidx int, tidx int) {
				trg := &g.Vertices[vidx]
				newinfo := frame.MessageAggregator(trg, uint32(vidx), uint32(vidx), g.InitVal)
				if newinfo {
					g.MessageQ[trg.ToThreadIdx()] <- graph.Message[MsgType]{Sidx: uint32(vidx), Didx: uint32(vidx), Message: g.EmptyVal}
					acc[tidx] += 1
				}
			})
			for _, v := range acc {
				g.MsgSend[VOTES-1] += v
			}
		} else { // Target specific vertex: send the algorithm defined initial visit value as a message.
			sidx := g.VertexMap[g.SourceVertex]
			trg := &g.Vertices[sidx]
			newinfo := frame.MessageAggregator(trg, sidx, sidx, g.InitVal)
			if newinfo {
				g.MessageQ[g.Vertices[sidx].ToThreadIdx()] <- graph.Message[MsgType]{Sidx: sidx, Didx: sidx, Message: g.EmptyVal}
				g.MsgSend[VOTES-1] += 1
			}
		}
		g.TerminateVote[VOTES-1] = 1
		g.TerminateData[VOTES-1] = int64(g.MsgSend[VOTES-1])
		wg.Done()
	}()

	const MsgBundleSize = 256
	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			msgBuffer := make([]graph.Message[MsgType], MsgBundleSize)
			for {
				//g.Mutex.RLock() // If we want to lock for oracle comparisons
				msgCounter := 0
				//msgBuffer := msgBuffer[:MsgBundleSize]
				// read a batch of messages
			fillLoop:
				for ; msgCounter < MsgBundleSize; msgCounter++ {
					select {
					case msg := <-g.MessageQ[tidx]:
						msgBuffer[msgCounter] = msg
					default:
						break fillLoop
					}
				}
				//msgBuffer = msgBuffer[:msgCounter]

				// consume messages read
				if msgCounter != 0 {
					g.TerminateVote[tidx] = -1
					for i := 0; i < msgCounter; i++ {
						msg := msgBuffer[i]
						target := &g.Vertices[msg.Didx]
						// Messages inserted by OnQueueVisitAsync always contain EmptyVal
						if !frame.IsMsgEmpty(msg.Message) {
							frame.MessageAggregator(target, msg.Didx, msg.Sidx, msg.Message)
						}
						val := frame.AggregateRetrieve(target)

						frame.OnVisitVertex(g, msg.Didx, val)
					}
					g.MsgRecv[tidx] += uint32(msgCounter)
				} else {
					if frame.CheckTermination(g, tidx) {
						wg.Done()
						return
					}
				}

				//g.Mutex.RUnlock()
			}
		}(uint32(t), &wg)
	}
	wg.Wait()
	frame.EnsureCompleteness(g)
}

// CheckTermination: Checks for messages consumed == produced, and if so, votes to quit.
// If any thread generates new messages they will not vote to quit, update new messages sent,
// thus kick out others until cons = prod.
// Works because produced >= consumed at all times.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) CheckTermination(g *graph.Graph[VertexProp, EdgeProp, MsgType], tidx uint32) bool {
	VOTES := graph.THREADS + 1

	g.TerminateData[tidx] = int64(g.MsgSend[tidx]) - int64(g.MsgRecv[tidx])
	allMsgs := int64(0)
	for v := 0; v < VOTES; v++ {
		allMsgs += g.TerminateData[v]
	}
	if allMsgs == 0 {
		g.TerminateVote[tidx] = 1
		allDone := 0
		for v := 0; v < VOTES; v++ {
			allDone += g.TerminateVote[v]
		}
		if allDone == VOTES {
			return true
		}
	}
	return false
}

// EnsureCompleteness: Debug func to ensure queues are empty an no messages are inflight.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) EnsureCompleteness(g *graph.Graph[VertexProp, EdgeProp, MsgType]) {
	inFlight := int64(0)
	for v := 0; v < graph.THREADS+1; v++ {
		inFlight += g.TerminateData[v]
	}
	enforce.ENFORCE(inFlight == 0, g.TerminateData)

	for i := 0; i < graph.THREADS; i++ {
		select {
		case t, ok := <-g.MessageQ[i]:
			if ok {
				info(i, " ", t, "Leftover in queue?")
			} else {
				info(i, "Channel closed!")
			}
		default:
		}
	}
}

// PrintTerminationStatus: Debug func to periodically print termination data and vote status.
func PrintTerminationStatus[VertexProp, EdgeProp, MsgType any](g *graph.Graph[VertexProp, EdgeProp, MsgType], exit *bool) {
	time.Sleep(2 * time.Second)
	for !*exit {
		chktermData := make([]int64, graph.THREADS+1)
		chkRes := int64(0)
		for i := range chktermData {
			chktermData[i] = int64(g.MsgSend[i]) - int64(g.MsgRecv[i])
			chkRes += chktermData[i]
		}
		//info("Effective: ", chktermData)
		info("Outstanding:  ", chkRes, " Votes: ", g.TerminateVote)
		time.Sleep(5 * time.Second)
	}
}
