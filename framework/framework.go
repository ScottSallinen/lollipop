package framework

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

var resultCache []float64

func info(args ...interface{}) {
	log.Println("[Framework]\t", fmt.Sprint(args...))
}

type Framework struct {
	OnInitVertex       OnInitVertexFunc
	OnVisitVertex      OnVisitVertexFunc
	OnFinish           OnFinishFunc
	OnCheckCorrectness OnCheckCorrectnessFunc
	OnEdgeAdd          OnEdgeAddFunc
	OnEdgeDel          OnEdgeDelFunc
	MessageAggregator  MessageAggregatorFunc
	AggregateRetrieve  AggregateRetrieveFunc
}

type OnInitVertexFunc func(g *graph.Graph, vidx uint32)
type OnVisitVertexFunc func(g *graph.Graph, vidx uint32, data float64) int
type OnFinishFunc func(g *graph.Graph) error
type OnCheckCorrectnessFunc func(g *graph.Graph) error
type OnEdgeAddFunc func(g *graph.Graph, sidx uint32, didxs map[uint32]int, VisitData float64)
type OnEdgeDelFunc func(g *graph.Graph, sidx uint32, didx uint32, VisitData float64)
type MessageAggregatorFunc func(g *graph.Vertex, VisitData float64) (newInfo bool)
type AggregateRetrieveFunc func(g *graph.Vertex) (data float64)

func (frame *Framework) Init(g *graph.Graph, async bool, dynamic bool) {
	//info("Started.")
	if async || dynamic {
		g.OnQueueVisit = frame.OnQueueVisitAsync
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
				g.MessageQ[i] = make(chan graph.Message, (4 * 4096 * 64 * 64))
				g.ThreadStructureQ[i] = make(chan graph.StructureChange, 4*4*4096)
			} else {
				g.MessageQ[i] = make(chan graph.Message, len(g.Vertices)+8)
			}
		}
		g.OnQueueEdgeAddRev = OnQueueEdgeAddRevAsync
		if dynamic {
			g.AlgConverge = frame.ConvergeAsyncDynWithRate
		} else {
			g.AlgConverge = frame.ConvergeAsync
		}
	} else {
		g.OnQueueVisit = frame.OnQueueVisitSync
		g.AlgConverge = frame.ConvergeSync
	}

	//m0 := time.Now()
	for vidx := range g.Vertices {
		frame.OnInitVertex(g, uint32(vidx))
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
	frame.OnFinish(g)
	//t2 := time.Since(m2)
	//info("Finalized(ms) ", t2.Milliseconds())

	err := frame.OnCheckCorrectness(g)
	enforce.ENFORCE(err)
	//info("Correct.")
	outputWg.Done()
}

func (frame *Framework) Launch(g *graph.Graph, gName string, async bool, dynamic bool, oracle bool) {
	if !dynamic {
		g.LoadGraphStatic(gName)
	}

	frame.Init(g, async, dynamic)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	if dynamic {
		go g.LoadGraphDynamic(gName, &feederWg)
	}

	if oracle {
		exit := false
		defer func() { exit = true }()
		go frame.CompareToOracleRunnable(g, &exit, 1000*time.Millisecond)
	}

	frame.Run(g, &feederWg, &frameWait)
}

func (frame *Framework) CompareToOracleRunnable(g *graph.Graph, exit *bool, sleepTime time.Duration) {
	time.Sleep(sleepTime)
	for !*exit {
		frame.CompareToOracle(g)
		time.Sleep(sleepTime)
	}
}

func (originalFrame *Framework) CompareToOracle(g *graph.Graph) {
	numEdges := uint64(0)

	g.Mutex.Lock()
	g.Watch.Pause()
	info("----INLINE----")
	info("inlineCurrentTime(ms) ", g.Watch.Elapsed().Milliseconds())
	newFrame := Framework{}
	newFrame.OnInitVertex = originalFrame.OnInitVertex
	newFrame.OnVisitVertex = originalFrame.OnVisitVertex
	newFrame.OnFinish = originalFrame.OnFinish
	newFrame.OnCheckCorrectness = originalFrame.OnCheckCorrectness
	newFrame.OnEdgeAdd = originalFrame.OnEdgeAdd
	newFrame.OnEdgeDel = originalFrame.OnEdgeDel
	newFrame.MessageAggregator = originalFrame.MessageAggregator
	newFrame.AggregateRetrieve = originalFrame.AggregateRetrieve

	altG := &graph.Graph{}
	altG.EmptyVal = g.EmptyVal
	altG.SourceInit = g.SourceInit
	altG.SourceInitVal = g.SourceInitVal
	altG.SourceVertex = g.SourceVertex

	altG.VertexMap = g.VertexMap // ok to shallow copy, we do not edit.
	altG.Vertices = make([]graph.Vertex, len(g.Vertices))
	gVertexStash := make([]graph.Vertex, len(g.Vertices))
	for v := range g.Vertices {
		altG.Vertices[v].Id = g.Vertices[v].Id
		altG.Vertices[v].OutEdges = g.Vertices[v].OutEdges
		numEdges += uint64(len(g.Vertices[v].OutEdges))
		gVertexStash[v].Value = g.Vertices[v].Value
		gVertexStash[v].Id = g.Vertices[v].Id
		gVertexStash[v].Residual = g.Vertices[v].Residual
		gVertexStash[v].Scratch = g.Vertices[v].Scratch
	}

	if resultCache == nil {
		newFrame.Init(altG, true, false)
		var feederWg sync.WaitGroup
		feederWg.Add(1)
		var frameWait sync.WaitGroup
		frameWait.Add(1)
		newFrame.Run(altG, &feederWg, &frameWait)
	}

	ia := make([]float64, len(g.Vertices))
	ib := make([]float64, len(g.Vertices))

	// Here we "early finish" proper G immediately for a fair comparison (i.e., including sink adjustment)
	// to compare a fully finished to the current state. Since the OnFinish is minute in cost but big in effect,
	// important to compare with it applied to both .
	newFrame.OnFinish(g)

	for v := range g.Vertices {
		ia[v] = altG.Vertices[v].Value
		ib[v] = g.Vertices[v].Value
		// Resetting the effect of the "early finish"
		g.Vertices[v].Value = gVertexStash[v].Value
		g.Vertices[v].Residual = gVertexStash[v].Residual
		g.Vertices[v].Scratch = gVertexStash[v].Scratch
	}
	const ORACLEEDGES = 28511807
	const ORACLEVERTICES = 1791489

	if resultCache == nil && numEdges == ORACLEEDGES {
		resultCache = make([]float64, len(ia))
		copy(resultCache, ia)
	}
	if resultCache != nil {
		copy(ia, resultCache)
	}
	info("vertexCount ", uint64(len(g.Vertices)), " edgeCount ", numEdges, " vertexPct ", (len(g.Vertices)*100)/ORACLEVERTICES, " edgePct ", (numEdges*100)/ORACLEEDGES)
	graph.ResultCompare(ia, ib)

	iaRank := mathutils.NewIndexedFloat64Slice(ia)
	ibRank := mathutils.NewIndexedFloat64Slice(ib)
	sort.Sort(sort.Reverse(iaRank))
	sort.Sort(sort.Reverse(ibRank))

	topN := 1000
	topK := 100
	if len(iaRank.Idx) < topN {
		topN = len(iaRank.Idx)
	}
	if len(iaRank.Idx) < topK {
		topK = len(iaRank.Idx)
	}
	iaRk := make([]int, topK)
	copy(iaRk, iaRank.Idx[:topK])
	ibRk := make([]int, topK)
	copy(ibRk, ibRank.Idx[:topK])

	mRBO6 := mathutils.CalculateRBO(iaRank.Idx[:topN], ibRank.Idx[:topN], 0.6)
	mRBO9 := mathutils.CalculateRBO(iaRk, ibRk, 0.9)
	info("top", topN, " RBO6 ", fmt.Sprintf("%.4f", mRBO6*100.0), " top", topK, " RBO9 ", fmt.Sprintf("%.4f", mRBO9*100.0))

	/*
		// This currently does not work.. we would need to pull the queues and then add them back.
		// Do not know if that is feasible.

		// Next test, how long to finish G from its current state?
		mirrorG := &graph.Graph{}
		mirrorG.OnInitVertex = OnInitVertex

		mirrorG.VertexMap = g.VertexMap // ok to shallow copy, we do not edit.
		mirrorG.Vertices = make([]graph.Vertex, len(g.Vertices))
		for v := range g.Vertices {
			mirrorG.Vertices[v].OutEdges = g.Vertices[v].OutEdges // shallow
			mirrorG.Vertices[v].Id = g.Vertices[v].Id
			mirrorG.Vertices[v].Properties.Residual = g.Vertices[v].Properties.Residual
			mirrorG.Vertices[v].Properties.Value = g.Vertices[v].Properties.Value
			mirrorG.Vertices[v].Active = g.Vertices[v].Active
			mirrorG.Vertices[v].Scratch = g.Vertices[v].Scratch
		}

		frame.Init(mirrorG, true, false)
		mirrorG.MessageQ = make([]chan graph.Message, graph.THREADS)
		for i := 0; i < graph.THREADS; i++ {
			mirrorG.MsgSend[i] = g.MsgSend[i]
			mirrorG.MsgRecv[i] = g.MsgRecv[i]
			mirrorG.MessageQ[i] = g.MessageQ[i]
		}

		var mirrorGfeederWg sync.WaitGroup
		mirrorGfeederWg.Add(1)
		var mirrorGframeWait sync.WaitGroup
		mirrorGframeWait.Add(1)

		frame.Run(mirrorG, &mirrorGfeederWg, &mirrorGframeWait)
	*/

	g.Mutex.Unlock()
	info("----END_INLINE----")
	g.Watch.UnPause()
}
