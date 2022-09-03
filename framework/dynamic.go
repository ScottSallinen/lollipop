package framework

import (
	"fmt"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func (frame *Framework[VertexProp, EdgeProp, MsgType]) EnactStructureChanges(g *graph.Graph[VertexProp, EdgeProp, MsgType], tidx uint32, changes []graph.StructureChange[EdgeProp]) {
	hasChangedIdMapping := false
	newVid := make(map[uint32]bool, len(changes)*2)
	miniGraph := make(map[uint32][]graph.StructureChange[EdgeProp], len(changes))

	// First pass: read lock the graph (no changes, just need consistent view).
	g.Mutex.RLock()
	for _, change := range changes {
		// Gather changes to a given source vertex. We use raw IDs as they may not exist yet.
		miniGraph[change.SrcRaw] = append(miniGraph[change.SrcRaw], change)
		_, srcOk := g.VertexMap[change.SrcRaw]
		_, dstOk := g.VertexMap[change.DstRaw]
		// If a given raw ID does not exist yet, keep track of it (uniquely).
		if !srcOk {
			newVid[change.SrcRaw] = true
			hasChangedIdMapping = true
		}
		if !dstOk {
			newVid[change.DstRaw] = true
			hasChangedIdMapping = true
		}
	}
	g.Mutex.RUnlock()

	// If we have a new raw ID to add to the graph, we need to write lock the graph.
	if hasChangedIdMapping {
		g.Mutex.Lock()
		for IdRaw := range newVid {
			// Here we can double check the existance of a raw ID.
			// Another thread may have already added it before we aquired the lock.
			_, idOk := g.VertexMap[IdRaw]
			if !idOk {
				// First, create vertex.
				vidx := uint32(len(g.VertexMap))
				g.VertexMap[uint32(IdRaw)] = vidx
				g.Vertices = append(g.Vertices, graph.Vertex[VertexProp, EdgeProp]{Id: IdRaw})
				frame.OnInitVertex(g, vidx)
				// Next, visit the newly created vertex if needed.
				if g.SourceInit && IdRaw == g.SourceVertex { // Only visit targetted vertex.
					frame.OnVisitVertex(g, vidx, g.InitVal)
				} else if !g.SourceInit { // We initial visit all vertices.
					frame.OnVisitVertex(g, vidx, g.InitVal)
				}
			}
		}
		g.Mutex.Unlock()
	}
	newVid = nil

	// Next, range over the newly added graph structure. Here we range over vertices.
	for vRaw := range miniGraph {
		g.Mutex.RLock()
		sidx := g.VertexMap[vRaw]
		src := &g.Vertices[sidx]
		// Here we loop over changes to a vertices edges.
		changeIdx := 0
		for changeIdx < len(miniGraph[vRaw]) {
			// First: gather any consecutive edge ADDs. This is because we wish to aggregate them.
			didxStart := len(src.OutEdges)
			for ; changeIdx < len(miniGraph[vRaw]); changeIdx++ {
				change := miniGraph[vRaw][changeIdx]
				if change.Type == graph.ADD {
					didx := g.VertexMap[change.DstRaw]
					src.OutEdges = append(src.OutEdges, graph.Edge[EdgeProp]{Property: change.EdgeProperty, Destination: didx})
				} else {
					// Was a delete; we will break early and address this changeIdx in a moment.
					break
				}
			}
			// From the gathered set of consecutive adds, apply them.
			if len(src.OutEdges) > didxStart {
				val := frame.AggregateRetrieve(src)
				msgs := frame.OnEdgeAdd(g, sidx, didxStart, val)
				if g.Undirected || g.SendRevMsgs {
					for eidx := didxStart; eidx < len(src.OutEdges); eidx++ {
						edge := src.OutEdges[eidx]
						didx := edge.Destination
						var msg MsgType
						if msgs != nil { // Just incase this is not needed/implemented
							msg = msgs[eidx-didxStart]
						}
						// Target dest since we swap s/d for the reverse
						// TODO: Some consideration needed for edgeproperties here... (should not be shared mem / pointers)
						g.ReverseMsgQ[g.RawIdToThreadIdx(g.Vertices[didx].Id)] <- graph.RevMessage[MsgType, EdgeProp]{Message: msg, EdgeProperty: edge.Property, Type: graph.ADD, Didx: sidx, Sidx: didx}
						// These are enqueued messages sent to be processed asynchronously, so we must use our MsgSend / Recv tracking.
					}
					g.MsgSend[tidx] += uint32(len(src.OutEdges) - didxStart)
				}
			}

			// If we didn't finish, it means we hit a delete. Address it here.
			if changeIdx < len(miniGraph[vRaw]) {
				change := miniGraph[vRaw][changeIdx]
				enforce.ENFORCE(change.Type == graph.DEL)
				didx := g.VertexMap[change.DstRaw]
				var prop EdgeProp
				/// Delete edge.. naively find target and swap last element with the hole.
				for k, v := range src.OutEdges {
					if v.Destination == didx {
						prop = src.OutEdges[k].Property
						src.OutEdges[k] = src.OutEdges[len(src.OutEdges)-1]
						break
					}
				}
				src.OutEdges = src.OutEdges[:len(src.OutEdges)-1]
				msg := frame.OnEdgeDel(g, sidx, didx, g.EmptyVal) // TODO: Merge visit?
				if g.Undirected || g.SendRevMsgs {
					g.ReverseMsgQ[g.RawIdToThreadIdx(change.DstRaw)] <- graph.RevMessage[MsgType, EdgeProp]{Message: msg, EdgeProperty: prop, Type: graph.DEL, Didx: sidx, Sidx: didx}
					g.MsgSend[tidx]++
				}
				changeIdx++
			}
			// Addressed the delete, continue the loop (go back to checking for consecutive adds).
		}
		g.Mutex.RUnlock()
	}
	miniGraph = nil
}

// Forward ensures the vertex exists (better optimized for shared memory).
// Here we just need to add the revese edge if undirected, otherwise call RevAdd / RevDel
// Note: SIDX AND DIDX HAVE ALREADY BEEN REVERSED. Source -> Dest is already reflecting the inverse edge.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) EnactReverseChanges(g *graph.Graph[VertexProp, EdgeProp, MsgType], tidx uint32, changes []graph.RevMessage[MsgType, EdgeProp]) {
	miniGraph := make(map[uint32][]graph.RevMessage[MsgType, EdgeProp], len(changes))
	for _, change := range changes {
		// Gather changes to a given source vertex.
		miniGraph[change.Sidx] = append(miniGraph[change.Sidx], change)
	}

	// Next, range over the newly added graph structure. Here we range over vertices.
	for sidx := range miniGraph {
		g.Mutex.RLock()
		src := &g.Vertices[sidx]
		// Here we loop over changes to a vertices edges.
		changeIdx := 0
		for changeIdx < len(miniGraph[sidx]) {
			// First: gather any consecutive edge ADDs. This is because we wish to aggregate them.
			didxStart := len(src.OutEdges)
			var sourceMsgs []MsgType
			for ; changeIdx < len(miniGraph[sidx]); changeIdx++ {
				change := miniGraph[sidx][changeIdx]
				if change.Type == graph.ADD {
					if g.Undirected {
						// TODO: Edge property considerations for undirected graph
						src.OutEdges = append(src.OutEdges, graph.Edge[EdgeProp]{Property: change.EdgeProperty, Destination: change.Didx})
					}
					sourceMsgs = append(sourceMsgs, change.Message)
				} else {
					// Was a delete; we will break early and address this changeIdx in a moment.
					break
				}
			}
			// From the gathered set of consecutive adds, apply them.
			if len(src.OutEdges) > didxStart {
				val := frame.AggregateRetrieve(src)
				frame.OnEdgeAddRev(g, sidx, didxStart, val, sourceMsgs)
				g.MsgRecv[tidx] += uint32(len(src.OutEdges) - didxStart)
			}

			// If we didn't finish, it means we hit a delete. Address it here.
			if changeIdx < len(miniGraph[sidx]) {
				change := miniGraph[sidx][changeIdx]
				enforce.ENFORCE(change.Type == graph.DEL)
				if g.Undirected { // Only delete if undirected graph.. otherwise just a notification of deletion.
					/// Delete edge.. naively find target and swap last element with the hole.
					for k, v := range src.OutEdges {
						if v.Destination == change.Didx {
							src.OutEdges[k] = src.OutEdges[len(src.OutEdges)-1]
							break
						}
					}
					src.OutEdges = src.OutEdges[:len(src.OutEdges)-1]
				}
				frame.OnEdgeDelRev(g, sidx, change.Didx, change.Message)
				g.MsgRecv[tidx]++
				changeIdx++
			}
			// Addressed the delete, continue the loop (go back to checking for consecutive adds).
		}
		g.Mutex.RUnlock()
	}
	miniGraph = nil
}

func (frame *Framework[VertexProp, EdgeProp, MsgType]) ProcessReverseEdges(g *graph.Graph[VertexProp, EdgeProp, MsgType], tidx uint32) {
	revBuffer := make([]graph.RevMessage[MsgType, EdgeProp], 0)
RevFillLoop:
	for { // Read a batch of Changes
		select {
		case msg, ok := <-g.ReverseMsgQ[tidx]:
			if ok {
				revBuffer = append(revBuffer, msg)
			} else {
				break RevFillLoop
			}
		default:
			break RevFillLoop
		}
	}
	if len(revBuffer) > 0 {
		frame.EnactReverseChanges(g, tidx, revBuffer)
	}
}

// ConvergeAsyncDynWithRate: Dynamic focused variant of async convergence.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) ConvergeAsyncDynWithRate(g *graph.Graph[VertexProp, EdgeProp, MsgType], feederWg *sync.WaitGroup) {
	info("ConvergeAsyncDynWithRate")
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	if graph.TARGETRATE == 0 {
		graph.TARGETRATE = 1e16
	}

	haltFlag := false
	if graph.DEBUG {
		exit := false
		defer func() { exit = true }()
		go PrintTerminationStatus(g, &exit)
	}

	// This adds a termination vote for when the dynamic injector is concluded.
	go func() {
		feederWg.Wait()
		wg.Done()
		g.TerminateVote[VOTES-1] = 1
	}()

	/* // For debugging if the termination isn't working.
		go func(hf *bool) {
			for !(*hf) {
				time.Sleep(10 * time.Millisecond)
			}
			wg.Done()
			g.TerminateVote[VOTES-1] = 1
		}(&haltFlag)
	/*/

	//m1 := time.Now()
	threadEdges := make([]uint64, graph.THREADS) // number of edges that each thread has processed

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			const MsgBundleSize = 256
			const GscBundleSize = 4096 * 16
			msgBuffer := make([]graph.Message[MsgType], MsgBundleSize)
			gscBuffer := make([]graph.StructureChange[EdgeProp], GscBundleSize)
			strucClosed := false // true indicates the StructureChanges channel is closed
			infoTimer := time.Now()
			for {
				// Process a batch of StructureChanges
				if !strucClosed && !haltFlag {
					//m2 := time.Since(m1)
					m2 := g.Watch.Elapsed()
					targetEdgeCount := m2.Seconds() * (float64(graph.TARGETRATE) / float64(graph.THREADS))
					incEdgeCount := uint64(targetEdgeCount) - threadEdges[tidx] // number of edges to process in this round

					ec := uint64(0) // edge count
				fillLoop:
					// Read a batch of StructureChanges
					for ; ec < incEdgeCount && ec < GscBundleSize; ec++ {
						select {
						case msg, ok := <-g.ThreadStructureQ[tidx]:
							if ok {
								gscBuffer[ec] = msg
							} else {
								if tidx == 0 {
									info("T0EdgeFinish ", g.Watch.Elapsed().Milliseconds())
								}
								strucClosed = true
								break fillLoop
							}
						default:
							break fillLoop
						}
					}
					if ec != 0 {
						frame.EnactStructureChanges(g, tidx, gscBuffer[:ec])
						threadEdges[tidx] += uint64(ec)

						allEdgeCount := uint64(0)
						for te := range threadEdges { // No need to lock, as we do not care for a consistent view, only approximate
							allEdgeCount += threadEdges[te]
							//if allEdgeCount > 18838563 {
							//	haltFlag = true
							//	strucClosed = true
							//	info("haltAt ", g.Watch.Elapsed().Milliseconds())
							//}
						}
						allRate := float64(allEdgeCount) / g.Watch.Elapsed().Seconds()

						if tidx == 0 && !strucClosed {
							infoTimerChk := time.Since(infoTimer)
							if infoTimerChk.Seconds() > 1.0 {
								info("ApproxRate ", uint64(allRate), " T0_IngestCount ", ec, " Percent ", fmt.Sprintf("%.3f", (float64(ec*100.0)/float64(incEdgeCount+1))), " AllRateAchieved ", fmt.Sprintf("%.3f", (allRate*100.0)/float64(graph.TARGETRATE)))
								infoTimer = time.Now()
							}
						}
						// Undirected or reverse messages, since we may not break this loop while ingesting forward edges
						if g.Undirected || g.SendRevMsgs {
							frame.ProcessReverseEdges(g, tidx)
						}

						if allRate/float64(graph.TARGETRATE) < 0.99 {
							continue
						}
					}
				}

				// Undirected or reverse messages
				// TODO: Probably want to skip this if we know we're actually done all forward edges.. but that's tricky to determine.
				// (every other thread must be done ingesting and finished processing, not just us, then our queue must be drained)
				if g.Undirected || g.SendRevMsgs {
					frame.ProcessReverseEdges(g, tidx)
				}

				// Process a batch of messages from the MessageQ
				algCount := 0
			algLoop:
				for ; algCount < MsgBundleSize; algCount++ {
					select {
					case msg := <-g.MessageQ[tidx]:
						msgBuffer[algCount] = msg
					default:
						break algLoop
					}
				}

				if algCount != 0 {
					g.TerminateVote[tidx] = -1
					g.Mutex.RLock()
					for i := 0; i < algCount; i++ {
						msg := msgBuffer[i]
						target := &g.Vertices[msg.Didx]
						// Messages inserted by OnQueueVisitAsync always contain EmptyVal
						if !frame.IsMsgEmpty(msg.Message) {
							frame.MessageAggregator(target, msg.Didx, msg.Sidx, msg.Message)
						}
						val := frame.AggregateRetrieve(target)

						//switch msg.Type {
						//case graph.ADD:
						//	enforce.ENFORCE(false)
						//case graph.DEL:
						//	enforce.ENFORCE(false)
						//case graph.VISIT:
						frame.OnVisitVertex(g, msg.Didx, val)
						//default:
						//	enforce.ENFORCE(false)
						//}
					}
					g.Mutex.RUnlock()
					g.MsgRecv[tidx] += uint32(algCount)
				} else if strucClosed { // No more structure changes (channel is closed)
					if frame.CheckTermination(g, tidx) {
						wg.Done()
						return
					}
				}
			}
		}(uint32(t), &wg)
	}
	wg.Wait()
	frame.EnsureCompleteness(g)
}
