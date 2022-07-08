package framework

import (
	"fmt"
	"log"
	"sync"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func info(args ...interface{}) {
	log.Println("[Framework]\t", fmt.Sprint(args...))
}

type Framework struct {
	OnVisitVertex      OnVisitVertexFunc
	OnFinish           OnFinishFunc
	OnCheckCorrectness OnCheckCorrectnessFunc
	OnEdgeAdd          OnEdgeAddFunc
	OnEdgeDel          OnEdgeDelFunc
	OnCompareOracle    OnCompareOracleFunc
}

type OnVisitVertexFunc func(g *graph.Graph, vidx uint32, data interface{}) int
type OnFinishFunc func(g *graph.Graph, data interface{}) error
type OnCheckCorrectnessFunc func(g *graph.Graph) error
type OnEdgeAddFunc func(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{})
type OnEdgeDelFunc func(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{})
type OnCompareOracleFunc func(g *graph.Graph)

func (frame *Framework) Init(g *graph.Graph, async bool, dynamic bool) {
	//info("Started.")
	if async {
		g.OnQueueVisit = OnQueueVisitAsync
		if dynamic {
			g.VertexMap = make(map[uint32]uint32, 4*4096)
		}
		g.MessageQ = make([]chan graph.Message, graph.THREADS)
		g.ThreadStructureQ = make([]chan graph.StructureChange, graph.THREADS)
		g.MsgSend = make([]uint32, graph.THREADS+1)
		g.MsgRecv = make([]uint32, graph.THREADS+1)
		g.TerminateVote = make([]int, graph.THREADS+1)
		g.TerminateData = make([]int64, graph.THREADS+1)
		for i := 0; i < graph.THREADS; i++ {
			if dynamic {
				// Need a better way to manipulate channel size for dynamic. Maybe request approx vertex count from user?
				g.MessageQ[i] = make(chan graph.Message, (4 * 4096 * graph.THREADS))
			} else {
				g.MessageQ[i] = make(chan graph.Message, len(g.Vertices))
			}
			g.ThreadStructureQ[i] = make(chan graph.StructureChange, 4*4096)
		}
		g.OnQueueEdgeAddRev = OnQueueEdgeAddRevAsync
		if dynamic {
			if graph.TARGETRATE != 0.0 {
				g.AlgConverge = frame.ConvergeAsyncDynWithRate
			} else {
				g.AlgConverge = frame.ConvergeAsyncDyn
			}
		} else {
			g.AlgConverge = frame.ConvergeAsync
		}
	} else {
		g.OnQueueVisit = OnQueueVisitSync
		g.AlgConverge = frame.ConvergeSync
	}

	//m0 := time.Now()
	for vidx := range g.Vertices {
		g.OnInitVertex(g, uint32(vidx), nil)
	}
	//t0 := time.Since(m0)

	//info("Initialized(ms) ", t0.Milliseconds())
}

func (frame *Framework) Run(g *graph.Graph, inputWg *sync.WaitGroup, outputWg *sync.WaitGroup) {
	//info("Running.")
	g.Watch.Start()
	g.AlgConverge(g, inputWg)
	//t1 := time.Since(m1)

	info("Termination(ms) ", g.Watch.Elapsed().Milliseconds(), " realtime(ms): ", g.Watch.AbsoluteElapsed().Milliseconds())

	//m2 := time.Now()
	frame.OnFinish(g, nil)
	//t2 := time.Since(m2)

	//info("Finalized(ms) ", t2.Milliseconds())

	err := frame.OnCheckCorrectness(g)
	if err != nil {
		enforce.ENFORCE(err)
	} else {
		//info("Correct.")
	}
	outputWg.Done()
}
