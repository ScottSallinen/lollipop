package framework

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

const GscBundleSize = 4 * 4096

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
					// Even though we will be the only one able to access this vertex, we will aggregate then retrieve immediately,
					// rather than directly send the initval as a visit -- this is to match the view that async algorithms have
					// on initialization (it flows through this process) and will let logic in aggregation modify the initial value if needed.
					frame.MessageAggregator(&g.Vertices[vidx], vidx, vidx, g.InitVal)
					initial := frame.AggregateRetrieve(&g.Vertices[vidx])
					frame.OnVisitVertex(g, vidx, initial)
				} else if !g.SourceInit { // We initial visit all vertices.
					frame.MessageAggregator(&g.Vertices[vidx], vidx, vidx, g.InitVal)
					initial := frame.AggregateRetrieve(&g.Vertices[vidx])
					frame.OnVisitVertex(g, vidx, initial)
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
				frame.OnEdgeAdd(g, sidx, didxStart, val)
			}

			// If we didn't finish, it means we hit a delete. Address it here.
			if changeIdx < len(miniGraph[vRaw]) {
				change := miniGraph[vRaw][changeIdx]
				enforce.ENFORCE(change.Type == graph.DEL)
				didx := g.VertexMap[change.DstRaw]
				/// Delete edge.. naively find target and swap last element with the hole.
				for k, v := range src.OutEdges {
					if v.Destination == didx {
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

	if graph.DEBUG { // For debugging if the termination isn't working.
		exit := false
		defer func() { exit = true }()
		go frame.PrintTerminationStatus(g, &exit)
	}

	go func() { // This adds a termination vote for when the dynamic injector is concluded.
		feederWg.Wait()
		wg.Done()
		g.TerminateVote[VOTES-1] = 1
	}()

	/*  haltFlag := false
		go func(hf *bool) {
			for !(*hf) {
				time.Sleep(10 * time.Millisecond)
			}
			wg.Done()
			g.TerminateVote[VOTES-1] = 1
		}(&haltFlag)
	/*/

	threadEdges := make([]int64, graph.THREADS) // Number of edges that each thread has processed.
	doneCounter := int32(0)                     // Counter for when a thread has no more edges left.

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			msgBuffer := make([]graph.Message[MsgType], MsgBundleSize)
			gscBuffer := make([]graph.StructureChange[EdgeProp], GscBundleSize)
			completed := false
			strucClosed := false // true indicates the StructureChanges channel is closed
			infoTimer := g.Watch.Elapsed().Seconds()
			allEdgeCountLast := int64(0)

			for !completed {
				// Process a batch of StructureChanges
				if !strucClosed {
					allEdgeCount := int64(0)
					for te := range threadEdges { // No need to lock, as we do not care for a consistent view, only approximate
						allEdgeCount += threadEdges[te]
					}
					//if allEdgeCount > 18838563 {
					//	haltFlag = true
					//	strucClosed = true
					//	info("haltAt ", g.Watch.Elapsed().Milliseconds())
					//}
					timeNow := g.Watch.Elapsed().Seconds()
					targetEdgeCount := int64(timeNow * (float64(graph.TARGETRATE)))
					incEdgeCount := (targetEdgeCount - allEdgeCount) / int64(graph.THREADS) // Target number of edges for this thread to process in this round
					allRate := float64(allEdgeCount) / timeNow

					if tidx == 0 && !strucClosed {
						if timeNow >= (infoTimer + 1.0) {
							allRateSince := float64(allEdgeCount-allEdgeCountLast) / 1.0
							info("TotalRate ", uint64(allRate), " InstRate ", uint64(allRateSince), " AllRateAchieved ", fmt.Sprintf("%.3f", (allRate*100.0)/float64(graph.TARGETRATE)))
							infoTimer = timeNow
							allEdgeCountLast = allEdgeCount
						}
					}

					edgeCount := int64(0)
				fillLoop: // Read a batch of StructureChanges
					for ; edgeCount < incEdgeCount && edgeCount < GscBundleSize; edgeCount++ {
						select {
						case msg, ok := <-g.ThreadStructureQ[tidx]:
							if ok {
								gscBuffer[edgeCount] = msg
							} else {
								doneCount := atomic.AddInt32(&doneCounter, 1)
								if doneCount == int32(graph.THREADS) {
									info("T", tidx, " all edges consumed at ", g.Watch.Elapsed().Milliseconds())
									//frame.CompareToOracle(g, false)
								}
								strucClosed = true
								break fillLoop
							}
						default:
							break fillLoop
						}
					}
					if edgeCount != 0 {
						frame.EnactStructureChanges(g, tidx, gscBuffer[:edgeCount])
						threadEdges[tidx] += int64(edgeCount)

						// If we are lagging behind the target ingestion rate, and we filled our bundle (implies more edges are available),
						// then we should loop back to ingest more edges rather than process algorithm messages.
						if allRate/float64(graph.TARGETRATE) < 0.99 && edgeCount == GscBundleSize {
							continue
						}
					}
				}

				// Process algorithm messages. Need to rlock, and check for termination only if we have no more edges to consume.
				completed = frame.ProcessMessages(g, tidx, msgBuffer, true, strucClosed)
			}
			wg.Done()
		}(uint32(t), &wg)
	}
	wg.Wait()
	frame.EnsureCompleteness(g)
}
