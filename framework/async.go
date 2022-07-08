package framework

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

//func OnQueueEdgeAddAsync(g *graph.Graph, SrcRaw uint32, DstRaw uint32, VisitData interface{}) {
//	g.ThreadStructureQ[g.RawIdToThreadIdx(SrcRaw)] <- graph.StructureChange{Type: graph.ADD, SrcRaw: SrcRaw, DstRaw: DstRaw}
//}

func EnactStructureChange(g *graph.Graph, frame *Framework, tidx uint32, changes []graph.StructureChange) {
	hasChanged := false
	newVid := make(map[uint32]bool)

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
			vidx, idOk := g.VertexMap[IdRaw]
			if !idOk {
				// First, create vertex.
				vidx = uint32(len(g.VertexMap))
				g.VertexMap[uint32(IdRaw)] = vidx
				g.Vertices = append(g.Vertices, graph.Vertex{Id: IdRaw})
				g.OnInitVertex(g, vidx, nil)
				// Next, visit the source vertex if needed.
				frame.OnVisitVertex(g, vidx, 0.0)
			}
		}
		g.Mutex.Unlock()
	}

	for _, change := range changes {
		g.Mutex.RLock()
		sidx := g.VertexMap[change.SrcRaw]
		didx := g.VertexMap[change.DstRaw]
		src := &g.Vertices[sidx]

		// Next, add the edge to the source vertex.
		src.Mutex.Lock()
		if change.Type == graph.ADD {
			/// Add edge.. simple
			src.OutEdges = append(src.OutEdges, graph.Edge{Target: didx})
		} else if change.Type == graph.DEL {
			/// Delete edge.. naively find target and swap last element with the hole.
			for k, v := range src.OutEdges {
				if v.Target == didx {
					src.OutEdges[k] = src.OutEdges[len(src.OutEdges)-1]
					break
				}
			}
			src.OutEdges = src.OutEdges[:len(src.OutEdges)-1]
		}
		val := float64(0)

		// TODO: Adjust this for delete as well?
		if change.Type == graph.ADD {
			val = mathutils.AtomicSwapFloat64(&src.Scratch, 0)
		}

		src.Mutex.Unlock()

		// Send the edge change message.
		if change.Type == graph.ADD {
			frame.OnEdgeAdd(g, sidx, didx, val)
		} else if change.Type == graph.DEL {
			frame.OnEdgeDel(g, sidx, didx, nil)
		}
		g.Mutex.RUnlock()
	}
}

// Todo: fix this when needed
func OnQueueEdgeAddRevAsync(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	// We message destination
	select {
	case g.MessageQ[g.Vertices[didx].ToThreadIdx()] <- graph.Message{Type: graph.ADDREV, Sidx: sidx, Didx: didx, Val: VisitData.(float64)}:
	default:
		enforce.ENFORCE(false, "queue error, tidx:", g.Vertices[didx].ToThreadIdx(), " filled to ", len(g.MessageQ[g.Vertices[didx].ToThreadIdx()]))
	}
	// Source increments message counter
	g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
}

func OnQueueVisitAsync(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	target := &g.Vertices[didx]

	mathutils.AtomicAddFloat64(&(target.Scratch), VisitData.(float64))

	// Maybe send message
	if !target.Active {
		// Double if to ensure only one applicant, as there may be multiple entrants here.
		target.Mutex.Lock()
		if !target.Active {
			select {
			case g.MessageQ[g.Vertices[didx].ToThreadIdx()] <- graph.Message{Type: graph.VISIT, Sidx: sidx, Didx: didx, Val: 0.0}:
			default:
				enforce.ENFORCE(false, "queue error, tidx:", g.Vertices[didx].ToThreadIdx(), " filled to ", len(g.MessageQ[g.Vertices[didx].ToThreadIdx()]))
			}
			g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
			target.Active = true
		}
		target.Mutex.Unlock()
	}
}

func (frame *Framework) ConvergeAsync(g *graph.Graph, feederWg *sync.WaitGroup) {
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	//exit := false
	//defer func() { exit = true }()
	//go PrintTerminationStatus(g, &exit)

	go func() {
		g.TerminateData[VOTES-1] = int64(len(g.Vertices)) // overestimate so we don't accidentally terminate early
		for vidx := range g.Vertices {
			trg := &g.Vertices[vidx]
			if !g.Vertices[vidx].Active {
				trg.Mutex.Lock()
				if !g.Vertices[vidx].Active {
					g.MessageQ[g.Vertices[vidx].ToThreadIdx()] <- graph.Message{Sidx: 0, Didx: uint32(vidx), Val: 0.0}
					g.MsgSend[VOTES-1] += 1
					g.Vertices[vidx].Active = true
				}
				trg.Mutex.Unlock()
			}
		}
		g.TerminateVote[VOTES-1] = 1
		g.TerminateData[VOTES-1] = int64(g.MsgSend[VOTES-1])
		wg.Done()
	}()

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			for {
				//g.Mutex.RLock()
				select {
				case msg, ok := <-g.MessageQ[tidx]:
					if ok {
						//enforce.ENFORCE(msg.Type == graph.VISIT)
						g.TerminateVote[tidx] = -1
						target := &g.Vertices[msg.Didx]
						target.Mutex.Lock()
						//val := target.Scratch
						//target.Scratch = 0
						val := mathutils.AtomicSwapFloat64(&target.Scratch, 0)
						target.Active = false
						target.Mutex.Unlock()

						frame.OnVisitVertex(g, msg.Didx, val)
						g.MsgRecv[tidx] += 1
					} else {
						enforce.ENFORCE(false, "Message channel closed!")
					}
				default:
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

func (frame *Framework) ConvergeAsyncDyn(g *graph.Graph, feederWg *sync.WaitGroup) {
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	//exit := false
	//defer func() { exit = true }()
	//go PrintTerminationStatus(g, &exit)

	go func() {
		feederWg.Wait()
		wg.Done()
		g.TerminateVote[VOTES-1] = 1
	}()

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			gsc := make([]graph.StructureChange, 0, 4096)
			strucClosed := false
			for {
				select {
				case msg := <-g.MessageQ[tidx]:
					g.TerminateVote[tidx] = -1

					g.Mutex.RLock()
					target := &g.Vertices[msg.Didx]
					target.Mutex.Lock()
					//val := target.Scratch
					//target.Scratch = 0
					val := mathutils.AtomicSwapFloat64(&target.Scratch, 0)
					target.Active = false
					target.Mutex.Unlock()

					switch msg.Type {
					case graph.ADD:
						enforce.ENFORCE(false)
						break
					case graph.DEL:
						enforce.ENFORCE(false)
						break
					case graph.VISIT:
						frame.OnVisitVertex(g, msg.Didx, (msg.Val + val))
						break
					default:
						enforce.ENFORCE(false)
					}
					g.Mutex.RUnlock()
					g.MsgRecv[tidx] += 1
				default:
					if strucClosed != true {
						gsc = gsc[:0] // reuse buffer
					fillLoop:
						for {
							select {
							case msg, ok := <-g.ThreadStructureQ[tidx]:
								if ok {
									gsc = append(gsc, msg)
								} else {
									strucClosed = true
									break fillLoop
								}
							default:
								break fillLoop
							}
						}

						if len(gsc) != 0 {
							EnactStructureChange(g, frame, tidx, gsc)
						}
					}

					if strucClosed { // No more structure changes (channel is closed)
						if frame.CheckTermination(g, tidx) {
							wg.Done()
							return
						}
					}
				}
			}
		}(uint32(t), &wg)
	}
	wg.Wait()
	frame.EnsureCompleteness(g)
}

func (frame *Framework) ConvergeAsyncDynWithRate(g *graph.Graph, feederWg *sync.WaitGroup) {
	info("ConvergeAsyncDynWithRate")
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	haltFlag := false
	//exit := false
	//defer func() { exit = true }()
	//go PrintTerminationStatus(g, &exit)

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
				if strucClosed != true && haltFlag != true {
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
								strucClosed = true
								break fillLoop
							}
						default:
							break fillLoop
						}
					}
					if len(gsc) != 0 {
						EnactStructureChange(g, frame, tidx, gsc)
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

						if tidx == 0 {
							infoTimerChk := time.Since(infoTimer)
							if infoTimerChk.Seconds() > 1.0 {
								info("Approx_rate: ", uint64(allRate), " T0_Ingest_pct(", ec, "/", incEdgeCount, ")eq: ", (ec/(incEdgeCount+1))*100.0, " rate_achieve_pct ", allRate/float64(graph.TARGETRATE))
								infoTimer = time.Now()
							}
						}

						if allRate/float64(graph.TARGETRATE) < 0.99 {
							continue
						}
					}
				}

				algCount := 0
				const algTarget = 10000
			algLoop:
				for ; algCount < algTarget; algCount++ {
					select {
					case msg := <-g.MessageQ[tidx]:
						g.TerminateVote[tidx] = -1

						g.Mutex.RLock()
						target := &g.Vertices[msg.Didx]
						target.Mutex.Lock()
						//val := target.Scratch
						//target.Scratch = 0
						val := mathutils.AtomicSwapFloat64(&target.Scratch, 0)
						target.Active = false
						target.Mutex.Unlock()

						switch msg.Type {
						case graph.ADD:
							enforce.ENFORCE(false)
							break
						case graph.DEL:
							enforce.ENFORCE(false)
							break
						case graph.VISIT:
							frame.OnVisitVertex(g, msg.Didx, (msg.Val + val))
							break
						default:
							enforce.ENFORCE(false)
						}
						g.Mutex.RUnlock()
						g.MsgRecv[tidx] += 1
					default:
						break algLoop
					}
				}

				if algCount < algTarget && strucClosed { // No more structure changes (channel is closed)
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

// CheckTermination: Checks for messages consumed == produced, and if so, votes to quit.
// If any thread generates new messages they will not vote to quit, update new messages sent,
// thus kick out others until cons = prod.
// Works because produced >= consumed at all times.
func (frame *Framework) CheckTermination(g *graph.Graph, tidx uint32) bool {
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

// EnsureCompleteness: Debug func to ensure queues are empty an no messages are inflight
func (frame *Framework) EnsureCompleteness(g *graph.Graph) {
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

// PrintTerminationStatus: Debug func to periodically print termination data and vote status
func PrintTerminationStatus(g *graph.Graph, exit *bool) {
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
