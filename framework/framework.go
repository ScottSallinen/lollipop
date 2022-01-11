package framework

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...interface{}) {
	log.Println("[Framework]\t", fmt.Sprint(args...))
}

const THREADS = 32

type Framework struct {
	OnInit             OnInitFunc
	OnVisitVertex      OnVisitVertexFunc
	OnFini             OnFiniFunc
	OnCheckCorrectness OnCheckCorrectnessFunc
}

type OnInitFunc func(g *graph.Graph, data interface{}) error
type OnVisitVertexFunc func(g *graph.Graph, vidx uint32, data interface{}) int
type OnFiniFunc func(g *graph.Graph, data interface{}) error
type OnCheckCorrectnessFunc func(g *graph.Graph) error

/** Sync Framework **/

func OnQueueVisitSync(g *graph.Graph, Src uint32, Dst uint32, VisitData interface{}) {
	target := &g.Vertices[Dst]
	target.Mu.Lock()
	target.Scratch += VisitData.(float64)
	target.Mu.Unlock()
}

func (frame *Framework) SyncConverge(g *graph.Graph) {
	iteration := 0
	for {
		vertexActive := 0
		var wg sync.WaitGroup
		wg.Add(THREADS)
		batch := (uint32(len(g.Vertices)) / THREADS)
		for i := uint32(0); i < THREADS; i++ {
			go func(idx uint32) {
				defer wg.Done()
				start := idx * batch
				end := (idx + 1) * batch
				if idx == (THREADS - 1) {
					end = uint32(len(g.Vertices))
				}
				for j := start; j < end; j++ {
					target := &g.Vertices[j]
					if target.Scratch != 0 {
						msgVal := 0.0
						target.Mu.Lock()
						msgVal = target.Scratch
						target.Scratch = 0
						target.Mu.Unlock()
						mActive := frame.OnVisitVertex(g, j, msgVal)
						if mActive > 0 {
							vertexActive = 1
						}
					}
				}
			}(i)
		}

		wg.Wait()
		iteration++
		if vertexActive != 1 {
			break
		}
	}
	info("Sync iterations: ", iteration)
}

/** Sync Framework **/

/** Async Framework **/

func OnQueueVisitAsync(g *graph.Graph, Src uint32, Dst uint32, VisitData interface{}) {
	target := &g.Vertices[Dst]
	target.Mu.Lock()
	// snd msg?
	if !target.Active {
		g.MessageQ[Dst%THREADS] <- graph.Message{Src: Src, Dst: Dst, Val: 0.0}
		g.MsgCounters[Src%THREADS] += 1
		target.Active = true
	}
	target.Scratch += VisitData.(float64)
	target.Mu.Unlock()
}

func (frame *Framework) AsyncConverge(g *graph.Graph) {
	var wg sync.WaitGroup
	VOTES := THREADS + 1
	exit := false
	wg.Add(VOTES)
	termVotes := make([]int, VOTES)
	termData := make([]int, VOTES)

	go func(exit *bool) {
		for !*exit && false {
			result := 0
			for _, v := range termData {
				result += v
			}
			info(termData, result)
			time.Sleep(1 * time.Second)
		}
	}(&exit)

	go func() {
		termData[VOTES-1] = len(g.Vertices) // overestimate
		for vidx := range g.Vertices {
			trg := &g.Vertices[vidx]
			if !g.Vertices[vidx].Active {
				trg.Mu.Lock()
				if !g.Vertices[vidx].Active {
					g.MessageQ[vidx%THREADS] <- graph.Message{Src: 0, Dst: uint32(vidx), Val: 0.0}
					g.MsgCounters[VOTES-1] += 1
					g.Vertices[vidx].Active = true
				}
				trg.Mu.Unlock()
			}
		}
		termVotes[VOTES-1] = 1
		termData[VOTES-1] = g.MsgCounters[VOTES-1]
		wg.Done()
	}()

	for i := 0; i < THREADS; i++ {
		go func(idx int, wg *sync.WaitGroup) {
			for {
				select {
				case msg, ok := <-g.MessageQ[idx]:
					if ok {
						termVotes[idx] = -1
						trg := &g.Vertices[msg.Dst]
						trg.Mu.Lock()
						val := trg.Scratch
						trg.Scratch = 0
						trg.Active = false
						trg.Mu.Unlock()
						frame.OnVisitVertex(g, msg.Dst, val)
						g.MsgCounters[idx] -= 1
					} else {
						info("Channel closed!")
					}
				default:
					termData[idx] = g.MsgCounters[idx]
					termVotes[idx] = 1
					allDone := 0
					allMsgs := 0
					for v := 0; v < VOTES; v++ {
						allDone += termVotes[v]
						allMsgs += termData[v]
					}
					if allDone == VOTES && allMsgs == 0 {
						wg.Done()
						return
					}
				}
			}
		}(i, &wg)
	}
	wg.Wait()
	exit = true

	inFlight := 0
	for v := 0; v < VOTES; v++ {
		//info(v, ":", termData[v])
		inFlight += termData[v]
	}
	enforce.ENFORCE(inFlight == 0)

	for i := 0; i < THREADS; i++ {
		select {
		case t, ok := <-g.MessageQ[i]:
			if ok {
				info(i, " ", t, "LEFTOVERS?")
			} else {
				info(i, "channel closed!")
			}
		default:
		}
	}
}

/** Async Framework **/

/** Async Pure Message Passing Framework **/

func OnQueueVisitAsyncPMP(g *graph.Graph, Src uint32, Dst uint32, VisitData interface{}) {
	g.MessageQ[Dst%THREADS] <- graph.Message{Src: Src, Dst: Dst, Val: VisitData.(float64)}
	g.MsgCounters[Src%THREADS] += 1
}

func (frame *Framework) AsyncPMPConverge(g *graph.Graph) {
	var wg sync.WaitGroup
	VOTES := THREADS + 1
	exit := false
	wg.Add(VOTES)
	termVotes := make([]int, VOTES)
	termData := make([]int, VOTES)

	go func(exit *bool) {
		for !*exit && false {
			result := 0
			for _, v := range termData {
				result += v
			}
			info(termData, result)
			time.Sleep(1 * time.Second)
		}
	}(&exit)

	go func() {
		termData[VOTES-1] = len(g.Vertices)
		for vidx := range g.Vertices {
			g.MessageQ[vidx%THREADS] <- graph.Message{Src: 0, Dst: uint32(vidx), Val: 1.0}
		}
		termVotes[VOTES-1] = 1
		wg.Done()
	}()

	for i := 0; i < THREADS; i++ {
		go func(idx int, wg *sync.WaitGroup) {
			msgs := make(map[uint32]float64, 0)
			termVotes[idx] = -1
			for {
				select {
				case msg, ok := <-g.MessageQ[idx]:
					if ok {
						msgs[msg.Dst] += msg.Val
						//frame.OnVisitVertex(g, msg.Dst, msg.Val)
						g.MsgCounters[idx] -= 1
					} else {
						info("Channel closed!")
					}
				default:
					if len(msgs) != 0 {
						termVotes[idx] = -1
						for k, v := range msgs {
							frame.OnVisitVertex(g, k, v)
						}
						msgs = make(map[uint32]float64, 0)
						continue
					}
					termData[idx] = g.MsgCounters[idx]
					termVotes[idx] = 1
					allDone := 0
					allMsgs := 0
					for v := 0; v < VOTES; v++ {
						allDone += termVotes[v]
						allMsgs += termData[v]
					}
					if allDone == VOTES && allMsgs == 0 {
						wg.Done()
						return
					}
				}
			}
		}(i, &wg)
	}
	wg.Wait()
	exit = true

	inFlight := 0
	for v := 0; v < VOTES; v++ {
		//info(v, ":", termData[v])
		inFlight += termData[v]
	}
	enforce.ENFORCE(inFlight == 0)

	for i := 0; i < THREADS; i++ {
		select {
		case t, ok := <-g.MessageQ[i]:
			if ok {
				info(i, " ", t, "LEFTOVERS?")
			} else {
				info(i, "channel closed!")
			}
		default:
		}
	}
}

/** Async Pure Message Passing Framework **/

func (frame *Framework) Run(g *graph.Graph, async bool) {
	info("Started.")
	if async {
		g.OnQueueVisit = OnQueueVisitAsync
		g.MessageQ = make([]chan graph.Message, THREADS)
		g.MsgCounters = make([]int, THREADS+1)
		for i := 0; i < THREADS; i++ {
			g.MessageQ[i] = make(chan graph.Message, len(g.Vertices))
		}
	} else {
		g.OnQueueVisit = OnQueueVisitSync
	}

	m0 := time.Now()
	frame.OnInit(g, nil)
	t0 := time.Since(m0)

	info("Initialized in ", t0)

	m1 := time.Now()
	if async {
		frame.AsyncConverge(g)
	} else {
		frame.SyncConverge(g)
	}
	t1 := time.Since(m1)

	info("Termination in ", t1)

	m2 := time.Now()
	frame.OnFini(g, nil)
	t2 := time.Since(m2)

	info("Finalized in ", t2)
	frame.OnCheckCorrectness(g)
	info("Correct.")
}
