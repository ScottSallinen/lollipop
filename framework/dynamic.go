package framework

import (
	"fmt"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func (frame *Framework) EnactStructureChange(g *graph.Graph, tidx uint32, changes []graph.StructureChange) {
	hasChanged := false
	newVid := make(map[uint32]bool, len(changes)*2)

	g.Mutex.RLock()
	for _, change := range changes {
		_, srcOk := g.VertexMap[change.SrcRaw]
		_, dstOk := g.VertexMap[change.DstRaw]
		if !srcOk {
			newVid[change.SrcRaw] = true
			hasChanged = true
		}
		if !dstOk {
			newVid[change.DstRaw] = true
			hasChanged = true
		}
	}
	g.Mutex.RUnlock()

	if hasChanged {
		g.Mutex.Lock()
		for IdRaw := range newVid {
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

	for _, change := range changes {
		g.Mutex.RLock()
		sidx := g.VertexMap[change.SrcRaw]
		didx := g.VertexMap[change.DstRaw]
		src := &g.Vertices[sidx]

		// Next, add the edge to the source vertex.
		//src.Mutex.Lock()
		if change.Type == graph.ADD {
			/// Add edge.. simple
			src.OutEdges = append(src.OutEdges, graph.Edge{Target: didx, Weight: change.Weight})
			//src.Mutex.Unlock()
			val := frame.AggregateRetrieve(src) // TODO: Adjust this for delete as well
			// Send the edge change message.
			frame.OnEdgeAdd(g, sidx, didx, val)
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
}

// Todo: fix this when needed
func OnQueueEdgeAddRevAsync(g *graph.Graph, sidx uint32, didx uint32, VisitData float64) {
	// We message destination
	select {
	case g.MessageQ[g.Vertices[didx].ToThreadIdx()] <- graph.Message{Type: graph.ADDREV, Sidx: sidx, Didx: didx, Val: VisitData}:
	default:
		enforce.ENFORCE(false, "queue error, tidx:", g.Vertices[didx].ToThreadIdx(), " filled to ", len(g.MessageQ[g.Vertices[didx].ToThreadIdx()]))
	}
	// Source increments message counter
	g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
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

	/*
		go func(hf *bool) {
			for !(*hf) {
				time.Sleep(10 * time.Millisecond)
			}
			wg.Done()
			g.TerminateVote[VOTES-1] = 1
		}(&haltFlag)
	*/

	//m1 := time.Now()
	threadEdges := make([]uint64, graph.THREADS)
	for te := range threadEdges {
		threadEdges[te] = 0
	}

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {

			gsc := make([]graph.StructureChange, 0, 4096)
			strucClosed := false
			infoTimer := time.Now()
			for {
				if !strucClosed && !haltFlag {
					//m2 := time.Since(m1)
					m2 := g.Watch.Elapsed()
					targetEdgeCount := m2.Seconds() * (float64(graph.TARGETRATE) / float64(graph.THREADS))
					incEdgeCount := uint64(targetEdgeCount) - threadEdges[tidx]

					gsc = gsc[:0] // reuse buffer
					ec := uint64(0)
				fillLoop:
					for ; ec < incEdgeCount; ec++ {
						select {
						case msg, ok := <-g.ThreadStructureQ[tidx]:
							if ok {
								gsc = append(gsc, msg)
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
					if len(gsc) != 0 {
						frame.EnactStructureChange(g, tidx, gsc)
						threadEdges[tidx] += uint64(len(gsc))

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
				const algTarget = 1000
				g.TerminateVote[tidx] = -1
				//seenV := make(map[uint32]uint32)
				g.Mutex.RLock()
			algLoop:
				for ; algCount < algTarget; algCount++ {
					select {
					case msg := <-g.MessageQ[tidx]:

						target := &g.Vertices[msg.Didx]
						//if c, ok := seenV[msg.Didx]; ok {
						//	info(tidx, ":", " re seen ", msg.Sidx, "->", msg.Didx, " count ", c)
						//}
						//seenV[msg.Didx] += 1
						//target.Mutex.Lock()
						if msg.Val != g.EmptyVal {
							frame.MessageAggregator(target, msg.Val)
						}
						val := frame.AggregateRetrieve(target)
						//target.Active = false
						//target.Mutex.Unlock()

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
						g.MsgRecv[tidx] += 1
					default:
						break algLoop
					}
				}
				g.Mutex.RUnlock()

				if (algCount < algTarget) && strucClosed { // No more structure changes (channel is closed)
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
