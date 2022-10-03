package framework

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

const MsgBundleSize = 256

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
		// Must be called by the source vertex's thread.
		g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
		select {
		case g.MessageQ[target.ToThreadIdx()] <- graph.Message[MsgType]{Type: graph.VISITEMPTYMSG, Sidx: sidx, Didx: didx, Message: g.EmptyVal}:
		default:
			enforce.ENFORCE(false, "queue error, tidx:", target.ToThreadIdx(), " filled to ", len(g.MessageQ[target.ToThreadIdx()]))
		}
	}
}

// Example function that would send a real message, to be accumulated on the reciever side (this is how a distributed set up would need to operate)
func (frame *Framework[VertexProp, EdgeProp, MsgType]) OnQueueVisitAsyncMsg(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitData MsgType) {
	target := &g.Vertices[didx]
	g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
	select {
	case g.MessageQ[target.ToThreadIdx()] <- graph.Message[MsgType]{Type: graph.VISIT, Sidx: sidx, Didx: didx, Message: VisitData}:
	default:
		enforce.ENFORCE(false, "queue error, tidx:", target.ToThreadIdx(), " filled to ", len(g.MessageQ[target.ToThreadIdx()]))
	}
	// must be called by the source vertex's thread
}

// SendInitialVisists: Enqueues the message(s) that will start the algorithm.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) SendInitialVisists(g *graph.Graph[VertexProp, EdgeProp, MsgType], VOTES int, wg *sync.WaitGroup) {
	if !g.SourceInit { // Target all vertices: send the algorithm defined initial visit value as a message.
		acc := make([]uint32, graph.THREADS)
		mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(vidx int, tidx int) {
			trg := &g.Vertices[vidx]
			newinfo := frame.MessageAggregator(trg, uint32(vidx), uint32(vidx), g.InitVal)
			if newinfo {
				g.MessageQ[trg.ToThreadIdx()] <- graph.Message[MsgType]{Type: graph.VISITEMPTYMSG, Sidx: uint32(vidx), Didx: uint32(vidx), Message: g.EmptyVal}
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
			g.MessageQ[g.Vertices[sidx].ToThreadIdx()] <- graph.Message[MsgType]{Type: graph.VISITEMPTYMSG, Sidx: sidx, Didx: sidx, Message: g.EmptyVal}
			g.MsgSend[VOTES-1] += 1
		}
	}
	g.TerminateVote[VOTES-1] = 1
	g.TerminateData[VOTES-1] = int64(g.MsgSend[VOTES-1])
	wg.Done()
}

// ProcessMessages will pull a bundle of messages targetting this thread, and then process them all.
// We can choose to readlock the graph if needed, and will check for termination only if the bool is set.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) ProcessMessages(g *graph.Graph[VertexProp, EdgeProp, MsgType], tidx uint32, msgBuffer []graph.Message[MsgType], lock bool, exitCheck bool) bool {
	algCount := 0
	// Process a batch of messages from the MessageQ
algLoop:
	for ; algCount < MsgBundleSize; algCount++ {
		select {
		case msg := <-g.MessageQ[tidx]:
			msgBuffer[algCount] = msg
			// Messages inserted by OnQueueVisitAsync are already aggregated from the sender side,
			// so no need to do so on the reciever side.
			// This exists here in case the message is sent as a normal visit with a real message,
			// so here we would be able to accumulate on the reciever side.
			//target := &g.Vertices[msg.Didx]
			//if msg.Type != graph.VISITEMPTYMSG {
			//	frame.MessageAggregator(target, msg.Didx, msg.Sidx, msg.Message)
			//}
		default:
			break algLoop
		}
	}

	// Consume messages read
	if algCount != 0 {
		g.TerminateVote[tidx] = -1
		if lock {
			g.Mutex.RLock()
		}
		for i := 0; i < algCount; i++ {
			msg := msgBuffer[i]
			val := frame.AggregateRetrieve(&g.Vertices[msg.Didx])
			//	if exitCheck && algCount < MsgBundleSize {
			//		info(msg.Sidx, "->", msg.Didx, ": ", val)
			//	}
			frame.OnVisitVertex(g, msg.Didx, val)
		}
		if lock {
			g.Mutex.RUnlock()
		}
		g.MsgRecv[tidx] += uint32(algCount)
	} else if exitCheck {
		if frame.CheckTermination(g, tidx) {
			return true
		}
	}
	return false
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
		go frame.PrintTerminationStatus(g, &exit)
	}

	g.TerminateData[VOTES-1] = int64(len(g.Vertices)) // overestimate so we don't accidentally terminate early. This is resolved when initial msgs are finished.
	// Send initial visit message(s)
	go frame.SendInitialVisists(g, VOTES, &wg)

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			msgBuffer := make([]graph.Message[MsgType], MsgBundleSize)
			completed := false
			for !completed {
				completed = frame.ProcessMessages(g, tidx, msgBuffer, false, true)
			}
			wg.Done()
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
func (frame *Framework[VertexProp, EdgeProp, MsgType]) PrintTerminationStatus(g *graph.Graph[VertexProp, EdgeProp, MsgType], exit *bool) {
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
