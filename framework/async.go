package framework

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
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
				vidx = uint32(len(g.VertexMap))
				g.VertexMap[uint32(IdRaw)] = vidx
				g.Vertices = append(g.Vertices, graph.Vertex{Id: IdRaw})
				g.OnInitVertex(g, vidx, nil)
			}
		}
		g.Mutex.Unlock()
	}

	for _, change := range changes {
		g.Mutex.RLock()
		sidx := g.VertexMap[change.SrcRaw]
		didx := g.VertexMap[change.DstRaw]
		src := &g.Vertices[sidx]
		src.Mutex.Lock()
		val := src.Scratch
		src.Scratch = 0
		src.Active = false
		src.Mutex.Unlock()
		frame.OnVisitVertex(g, sidx, val)
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
		src.Mutex.Unlock()
		if change.Type == graph.ADD {
			frame.OnEdgeAdd(g, sidx, didx, nil)
		} else {
			frame.OnEdgeDel(g, sidx, didx, nil)
		}
		g.Mutex.RUnlock()
	}
}

func OnQueueEdgeAddRevAsync(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	// We message destination
	select {
	case g.MessageQ[g.Vertices[didx].ToThreadIdx()] <- graph.Message{Type: graph.ADDREV, Sidx: sidx, Didx: didx, Val: VisitData.(float64)}:
	default:
		enforce.ENFORCE(false, "queue error")
	}
	// Source increments message counter
	g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
}

func OnQueueVisitAsync(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	target := &g.Vertices[didx]
	target.Mutex.Lock()
	target.Scratch += VisitData.(float64)
	// Maybe send message
	if !target.Active {
		select {
		case g.MessageQ[g.Vertices[didx].ToThreadIdx()] <- graph.Message{Type: graph.VISIT, Sidx: sidx, Didx: didx, Val: 0.0}:
		default:
			enforce.ENFORCE(false, "queue error")
		}
		//atomic.AddUint32(&g.MsgSend[g.Vertices[sidx].ToThreadIdx()], 1)
		g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
		target.Active = true
	}
	target.Mutex.Unlock()
}

func (frame *Framework) ConvergeAsync(g *graph.Graph, feederWg *sync.WaitGroup) {
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	wg.Add(VOTES)

	exit := false
	defer func() { exit = true }()
	go PrintTerminationStatus(g, &exit)

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
				select {
				case msg, ok := <-g.MessageQ[tidx]:
					if ok {
						//enforce.ENFORCE(msg.Type == graph.VISIT)
						g.TerminateVote[tidx] = -1
						target := &g.Vertices[msg.Didx]
						target.Mutex.Lock()
						val := target.Scratch
						target.Scratch = 0
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

	exit := false
	defer func() { exit = true }()
	go PrintTerminationStatus(g, &exit)

	go func() {
		feederWg.Wait()
		wg.Done()
		g.TerminateVote[VOTES-1] = 1
	}()

	for t := 0; t < graph.THREADS; t++ {
		go func(tidx uint32, wg *sync.WaitGroup) {
			gsc := make([]graph.StructureChange, 0, 4096)
			for {
				select {
				case msg := <-g.MessageQ[tidx]:
					g.TerminateVote[tidx] = -1

					g.Mutex.RLock()
					target := &g.Vertices[msg.Didx]
					target.Mutex.Lock()
					val := target.Scratch
					target.Scratch = 0
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
					gsc = gsc[:0] // reuse buffer
					strucClosed := false
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
					} else if strucClosed { // No more structure changes (channel is closed)
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
		info(chktermData, chkRes)
		info(g.TerminateVote)
		time.Sleep(5 * time.Second)
	}
}
