package framework

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

/// OnQueueVisitAsync: Async queue applying function; aggregates message values,
/// and only injects a visit marker if none exist already.
func (frame *Framework[VertexProp]) OnQueueVisitAsync(g *graph.Graph[VertexProp], sidx uint32, didx uint32, VisitData float64) {
	target := &g.Vertices[didx]

	//target.Mutex.Lock()
	doSendMessage := frame.MessageAggregator(target, VisitData)

	// Old direct way of doing things with locks here rather than in the algorithm.
	//if !target.Active {
	//	doSendMessage = true
	//	target.Active = true
	//}
	//target.Mutex.Unlock()

	// Having multiple visits for the same vertex in the queue at the same time is not ideal but possible. It is
	// difficult to avoid this without some cost. We just rely on the algorithm's OnInitVertex function to report
	// "no work to do" to avoid sending soon-to-be-discard message.
	if doSendMessage {
		select {
		case g.MessageQ[target.ToThreadIdx()] <- graph.Message{Type: graph.VISIT, Sidx: sidx, Didx: didx, Val: g.EmptyVal}:
		default:
			enforce.ENFORCE(false, "queue error, tidx:", target.ToThreadIdx(), " filled to ", len(g.MessageQ[target.ToThreadIdx()]))
		}
		// must be called by the source vertex's thread
		g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
	}
}

/// ConvergeAsync: Static focused variant of async convergence.
func (frame *Framework[VertexProp]) ConvergeAsync(g *graph.Graph[VertexProp], feederWg *sync.WaitGroup) {
	// TODO: feederWg not used?
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
		if !g.SourceInit { // Target all vertices: send an initial (empty) visit message.
			for vidx := range g.Vertices {
				trg := &g.Vertices[vidx]
				//if !trg.Active {
				//trg.Mutex.Lock()
				//if !trg.Active {
				if trg.Scratch == g.EmptyVal {
					g.MessageQ[trg.ToThreadIdx()] <- graph.Message{Sidx: uint32(vidx), Didx: uint32(vidx), Val: g.EmptyVal}
					g.MsgSend[VOTES-1] += 1
				}
				//trg.Active = true
				//}
				//trg.Mutex.Unlock()
				//}
			}
		} else { // Target specific vertex: send an initial visit message.
			sidx := g.VertexMap[g.SourceVertex]
			//trg := &g.Vertices[sidx]
			//trg.Mutex.Lock()
			g.MessageQ[g.Vertices[sidx].ToThreadIdx()] <- graph.Message{Sidx: sidx, Didx: sidx, Val: g.SourceInitVal}
			g.MsgSend[VOTES-1] += 1
			//g.Vertices[sidx].Active = true
			//trg.Mutex.Unlock()
		}
		g.TerminateVote[VOTES-1] = 1
		g.TerminateData[VOTES-1] = int64(g.MsgSend[VOTES-1])
		wg.Done()
	}()

	const MsgBundleSize = 256
	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			msgBuffer := make([]graph.Message, MsgBundleSize)
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
						// Non-empty value can only come from the initial visit message
						if msg.Val != g.EmptyVal {
							frame.MessageAggregator(target, msg.Val)
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

/// CheckTermination: Checks for messages consumed == produced, and if so, votes to quit.
/// If any thread generates new messages they will not vote to quit, update new messages sent,
/// thus kick out others until cons = prod.
/// Works because produced >= consumed at all times.
func (frame *Framework[VertexProp]) CheckTermination(g *graph.Graph[VertexProp], tidx uint32) bool {
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

/// EnsureCompleteness: Debug func to ensure queues are empty an no messages are inflight.
func (frame *Framework[VertexProp]) EnsureCompleteness(g *graph.Graph[VertexProp]) {
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

/// PrintTerminationStatus: Debug func to periodically print termination data and vote status.
func PrintTerminationStatus[VertexProp any](g *graph.Graph[VertexProp], exit *bool) {
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
