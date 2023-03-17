package framework

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

// TODO: make sure the use of g.Mutex does not have a significant impact on the performance

var tsLast = uint64(0)
var threadAtTimestamp []uint64

func (frame *Framework[VertexProp, EdgeProp, MsgType]) EnactStructureChanges(g *graph.Graph[VertexProp, EdgeProp, MsgType], tidx uint32, changes []graph.StructureChange[EdgeProp], expiredEdges *[]graph.StructureChange[EdgeProp]) {
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

	if g.Options.InsertDeleteOnExpire > 0 {
		for _, change := range changes {
			if g.Options.SkipDeleteProb > 0 && rand.Float64() < g.Options.SkipDeleteProb {
				continue
			}
			futureDelete := graph.StructureChange[EdgeProp]{Type: graph.DEL, SrcRaw: change.SrcRaw, DstRaw: change.DstRaw}
			tsInit := frame.GetTimestamp(change.EdgeProperty)
			frame.SetTimestamp(&futureDelete.EdgeProperty, tsInit+g.Options.InsertDeleteOnExpire)
			(*expiredEdges) = append((*expiredEdges), futureDelete)
		}

		for {
			if len((*expiredEdges)) == 0 {
				break
			}
			change := (*expiredEdges)[0]
			ts := frame.GetTimestamp(change.EdgeProperty)
			if ts <= threadAtTimestamp[tidx] {
				miniGraph[change.SrcRaw] = append(miniGraph[change.SrcRaw], change)
				(*expiredEdges) = (*expiredEdges)[1:]
				continue
			}
			break
		}
	}

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
				if g.Options.SourceInit { // Only visit targeted vertex.
					if message, ok := g.Options.InitMessages[IdRaw]; ok {
						// Even though we will be the only one able to access this vertex, we will aggregate then retrieve immediately,
						// rather than directly send the initial message as a visit -- this is to match the view that async algorithms have
						// on initialization (it flows through this process) and will let logic in aggregation modify the initial value if needed.
						frame.MessageAggregator(&g.Vertices[vidx], vidx, vidx, message)
						initial := frame.AggregateRetrieve(&g.Vertices[vidx])
						frame.OnVisitVertex(g, vidx, initial)
					}
				} else { // We initial visit all vertices.
					frame.MessageAggregator(&g.Vertices[vidx], vidx, vidx, g.Options.InitAllMessage)
					initial := frame.AggregateRetrieve(&g.Vertices[vidx])
					frame.OnVisitVertex(g, vidx, initial)
				}
			}
		}
		g.Mutex.Unlock()
	}
	newVid = nil

	latestTime := tsLast

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
					g.ThreadEdges[tidx] += 1
					didx := g.VertexMap[change.DstRaw]
					edge := graph.Edge[EdgeProp]{Property: change.EdgeProperty, Destination: didx}
					if g.Options.LogTimeseries || (g.Options.InsertDeleteOnExpire > 0) {
						latestTime = mathutils.Max(frame.GetTimestamp(edge.Property), latestTime)
						threadAtTimestamp[tidx] = latestTime
					}
					src.OutEdges = append(src.OutEdges, edge)
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

			var deletedEdges []graph.Edge[EdgeProp]
			// If we didn't finish, it means we hit a delete. Address it here.
			for ; changeIdx < len(miniGraph[vRaw]); changeIdx++ {
				change := miniGraph[vRaw][changeIdx]
				if change.Type == graph.DEL {
					g.ThreadEdges[tidx] -= 1
					didx := g.VertexMap[change.DstRaw]
					/// Delete edge.. naively find target and swap last element with the hole.
					for k := range src.OutEdges {
						if src.OutEdges[k].Destination == didx { // TODO: Multigraph target ?  compare property?
							deletedEdges = append(deletedEdges, src.OutEdges[k])
							src.OutEdges[k] = src.OutEdges[len(src.OutEdges)-1]
							break
						}
					}
					src.OutEdges = src.OutEdges[:len(src.OutEdges)-1]
				} else {
					break
				}
			}
			// From the gathered set of consecutive deletes, apply them.
			if len(deletedEdges) > 0 {
				val := frame.AggregateRetrieve(src)
				frame.OnEdgeDel(g, sidx, deletedEdges, val)
			}
			// Addressed the delete(s), continue the loop (go back to checking for consecutive adds).
		}
		g.Mutex.RUnlock()
	}

	if g.Options.LogTimeseries {
		last := tsLast
		potNextTime := (last + g.Options.TimeSeriesInterval)
		if latestTime > potNextTime {
			if atomic.CompareAndSwapUint64(&tsLast, last, latestTime) {
				g.LogEntryChan <- time.Unix(int64(latestTime), 0)
			}
		}
	}
	miniGraph = nil
}

// ConvergeAsyncDynWithRate: Dynamic focused variant of async convergence.
func (frame *Framework[VertexProp, EdgeProp, MsgType]) ConvergeAsyncDynWithRate(g *graph.Graph[VertexProp, EdgeProp, MsgType], feederWg *sync.WaitGroup) {
	info("ConvergeAsyncDynWithRate")
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	threadAtTimestamp = make([]uint64, graph.THREADS)

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
			msgBuffer := make([]graph.Message[MsgType], graph.MsgBundleSize)
			structureChangeBuffer := make([]graph.StructureChange[EdgeProp], graph.GscBundleSize)
			expiredEdges := make([]graph.StructureChange[EdgeProp], 0)
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
							allRateSince := float64(allEdgeCount-allEdgeCountLast) / (timeNow - infoTimer)
							if graph.TARGETRATE == 1e16 {
								info("Edges ", allEdgeCount, " TotalRate ", uint64(allRate), " CurrentRate ", uint64(allRateSince))
							} else {
								info("Edges ", allEdgeCount, " TotalRate ", uint64(allRate), " CurrentRate ", uint64(allRateSince), " AllRateAchieved ", fmt.Sprintf("%.3f", (allRate*100.0)/float64(graph.TARGETRATE)))
							}
							infoTimer = timeNow
							allEdgeCountLast = allEdgeCount
						}
					}

					edgeCount := int64(0)
				fillLoop: // Read a batch of StructureChanges
					for ; edgeCount < incEdgeCount && edgeCount < int64(len(structureChangeBuffer)); edgeCount++ {
						select {
						case msg, ok := <-g.ThreadStructureQ[tidx]:
							if ok {
								structureChangeBuffer[edgeCount] = msg
							} else {
								doneCount := atomic.AddInt32(&doneCounter, 1)
								if doneCount == int32(graph.THREADS) {
									info("T", tidx, " all edges consumed at ", g.Watch.Elapsed().Milliseconds())
									if g.Options.OracleCompare {
										go frame.CompareToOracle(g, true, false, time.Duration(g.Watch.Elapsed().Milliseconds()/10))
									}
								}
								strucClosed = true
								break fillLoop
							}
						default:
							break fillLoop
						}
					}
					if edgeCount != 0 {
						frame.EnactStructureChanges(g, tidx, structureChangeBuffer[:edgeCount], &expiredEdges)
						threadEdges[tidx] += int64(edgeCount)

						// If we are lagging behind the target ingestion rate, and we filled our bundle (implies more edges are available),
						// then we should loop back to ingest more edges rather than process algorithm messages.
						if allRate/float64(graph.TARGETRATE) < 0.99 && edgeCount == int64(len(structureChangeBuffer)) {
							continue
						}
					}
				}

				// Need to move rlock outside to avoid buffered msgs when comparing to oracle or logging a timeseries.
				// TODO: always setting rLockOutside to true would hypothetically results in better performance
				rLockOutside := g.Options.OracleCompare || g.Options.LogTimeseries || g.Options.ReadLockRequired
				if rLockOutside {
					g.Mutex.RLock()
				}
				// Process algorithm messages. Need to rlock (due to potential graph structure changes)
				// Also will only check for termination if we have no more edges to consume.
				completed = frame.ProcessMessages(g, tidx, msgBuffer, !rLockOutside, strucClosed, false)
				if rLockOutside {
					g.Mutex.RUnlock()
				}
			}
			wg.Done()
		}(uint32(t), &wg)
	}
	wg.Wait()
	frame.EnsureCompleteness(g)
}
