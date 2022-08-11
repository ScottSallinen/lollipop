package framework

import (
	"fmt"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func (frame *Framework) EnactStructureChanges(g *graph.Graph, tidx uint32, changes []graph.StructureChange) {
	hasChangedIdMapping := false
	newVid := make(map[uint32]bool, len(changes)*2)
	miniGraph := make(map[uint32][]graph.StructureChange, len(changes))

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
				g.Vertices = append(g.Vertices, graph.Vertex{Id: IdRaw})
				frame.OnInitVertex(g, vidx)
				// Next, visit the newly created vertex if needed.
				if g.SourceInit && IdRaw == g.SourceVertex { // Only visit targetted vertex.
					frame.OnVisitVertex(g, vidx, g.SourceInitVal)
				} else { // We initial visit all vertices.
					frame.OnVisitVertex(g, vidx, g.EmptyVal)
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
			didxMap := make(map[uint32]int, len(miniGraph[vRaw]))
			for ; changeIdx < len(miniGraph[vRaw]); changeIdx++ {
				change := miniGraph[vRaw][changeIdx]
				if change.Type == graph.ADD {
					didx := g.VertexMap[change.DstRaw]
					didxMap[didx] = len(src.OutEdges)
					src.OutEdges = append(src.OutEdges, graph.NewEdge(didx, change.Weight))
				} else {
					// Was a delete; we will break early and address this changeIdx in a moment.
					break
				}
			}
			// From the gathered set of consecutive adds, apply them.
			if len(didxMap) > 0 {
				val := frame.AggregateRetrieve(src)
				frame.OnEdgeAdd(g, sidx, didxMap, val)
			}

			// If we didn't finish, it means we hit a delete. Address it here.
			if changeIdx < len(miniGraph[vRaw]) {
				change := miniGraph[vRaw][changeIdx]
				enforce.ENFORCE(change.Type == graph.DEL)
				didx := g.VertexMap[change.DstRaw]
				/// Delete edge.. naively find target and swap last element with the hole.
				for k, v := range src.OutEdges {
					if v.Target == didx {
						src.OutEdges[k] = src.OutEdges[len(src.OutEdges)-1]
						break
					}
				}
				src.OutEdges = src.OutEdges[:len(src.OutEdges)-1]
				frame.OnEdgeDel(g, sidx, didx, g.EmptyVal)
				changeIdx++
			}
			// Addressed the delete, continue the loop (go back to checking for consecutive adds).
		}
		g.Mutex.RUnlock()
	}
	miniGraph = nil

	/* // Old basic version that does not aggregate messages.
	for _, change := range changes {
		g.Mutex.RLock()
		sidx := g.VertexMap[change.SrcRaw]
		didx := g.VertexMap[change.DstRaw]
		src := &g.Vertices[sidx]

		// Next, add the edge to the source vertex.
		//src.Mutex.Lock()
		if change.Type == graph.ADD {
			didxm := make(map[uint32]int)
			didxm[didx] = len(src.OutEdges)
			/// Add edge.. simple
			src.OutEdges = append(src.OutEdges, graph.Edge{Target: didx, Weight: change.Weight})
			//src.Mutex.Unlock()
			val := frame.AggregateRetrieve(src) // TODO: Adjust this for delete as well
			// Send the edge change message.
			frame.OnEdgeAdd(g, sidx, didxm, val)
		} else if change.Type == graph.DEL {
			/// Delete edge.. naively find target and swap last element with the hole.
			for k, v := range src.OutEdges {
				if v.Target == didx {
					src.OutEdges[k] = src.OutEdges[len(src.OutEdges)-1]
					break
				}
			}
			src.OutEdges = src.OutEdges[:len(src.OutEdges)-1]
			//src.Mutex.Unlock()
			// Send the edge change message.
			frame.OnEdgeDel(g, sidx, didx, g.EmptyVal)
		}
		g.Mutex.RUnlock()
	}
	//*/
}

// Todo: fix this when needed
func OnQueueEdgeAddRevAsync(g *graph.Graph, sidx uint32, didx uint32, VisitData float64) {
}

/// ConvergeAsyncDynWithRate: Dynamic focused variant of async convergence.
func (frame *Framework) ConvergeAsyncDynWithRate(g *graph.Graph, feederWg *sync.WaitGroup) {
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
	threadEdges := make([]uint64, graph.THREADS)
	for te := range threadEdges {
		threadEdges[te] = 0
	}

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			const MsgBundleSize = 256
			const GscBundleSize = 4096 * 16
			msgBuffer := make([]graph.Message, MsgBundleSize)
			gscBuffer := make([]graph.StructureChange, GscBundleSize)
			strucClosed := false
			infoTimer := time.Now()
			for {
				if !strucClosed && !haltFlag {
					//m2 := time.Since(m1)
					m2 := g.Watch.Elapsed()
					targetEdgeCount := m2.Seconds() * (float64(graph.TARGETRATE) / float64(graph.THREADS))
					incEdgeCount := uint64(targetEdgeCount) - threadEdges[tidx]

					ec := uint64(0)
				fillLoop:
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
						for te := range threadEdges {
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

						if allRate/float64(graph.TARGETRATE) < 0.99 {
							continue
						}
					}
				}

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
						if msg.Val != g.EmptyVal {
							frame.MessageAggregator(target, msg.Val)
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
