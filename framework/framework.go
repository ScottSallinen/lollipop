package framework

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

var resultCache []float64

func info(args ...any) {
	log.Println("[Framework]\t", fmt.Sprint(args...))
}

type Framework[VertexProp, EdgeProp any] struct {
	OnInitVertex       OnInitVertexFunc[VertexProp, EdgeProp]
	OnVisitVertex      OnVisitVertexFunc[VertexProp, EdgeProp]
	OnFinish           OnFinishFunc[VertexProp, EdgeProp]
	OnCheckCorrectness OnCheckCorrectnessFunc[VertexProp, EdgeProp]
	OnEdgeAdd          OnEdgeAddFunc[VertexProp, EdgeProp]
	OnEdgeDel          OnEdgeDelFunc[VertexProp, EdgeProp]
	MessageAggregator  MessageAggregatorFunc[VertexProp, EdgeProp]
	AggregateRetrieve  AggregateRetrieveFunc[VertexProp, EdgeProp]
	OracleComparison   OracleComparison[VertexProp, EdgeProp]
	EdgeParser         EdgeParserFunc[EdgeProp]
}

type OnInitVertexFunc[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp], vidx uint32)
type OnVisitVertexFunc[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp], vidx uint32, data float64) int
type OnFinishFunc[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp]) error
type OnCheckCorrectnessFunc[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp]) error
type OnEdgeAddFunc[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp], sidx uint32, didxStart int, VisitData float64)
type OnEdgeDelFunc[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp], sidx uint32, didx uint32, VisitData float64)
type MessageAggregatorFunc[VertexProp, EdgeProp any] func(dst, src *graph.Vertex[VertexProp, EdgeProp], VisitData float64) (newInfo bool)
type AggregateRetrieveFunc[VertexProp, EdgeProp any] func(g *graph.Vertex[VertexProp, EdgeProp]) (data float64)
type OracleComparison[VertexProp, EdgeProp any] func(g *graph.Graph[VertexProp, EdgeProp], oracle *graph.Graph[VertexProp, EdgeProp], resultCache *[]float64)
type EdgeParserFunc[EdgeProp any] graph.EdgeParserFunc[EdgeProp]

func (frame *Framework[VertexProp, EdgeProp]) Init(g *graph.Graph[VertexProp, EdgeProp], async bool, dynamic bool) {
	//info("Started.")
	if async || dynamic {
		g.OnQueueVisit = frame.OnQueueVisitAsync
		if dynamic {
			g.VertexMap = make(map[uint32]uint32, 4*4096)
		}
		g.MessageQ = make([]chan graph.Message, graph.THREADS)
		g.ThreadStructureQ = make([]chan graph.StructureChange[EdgeProp], graph.THREADS)
		g.MsgSend = make([]uint32, graph.THREADS+1)
		g.MsgRecv = make([]uint32, graph.THREADS+1)
		g.TerminateVote = make([]int, graph.THREADS+1)
		g.TerminateData = make([]int64, graph.THREADS+1)
		for i := 0; i < graph.THREADS; i++ {
			if dynamic {
				// TODO: Need a better way to manipulate channel size for dynamic. Maybe request approx vertex count from user?
				g.MessageQ[i] = make(chan graph.Message, (4 * 4096 * 64))
				g.ThreadStructureQ[i] = make(chan graph.StructureChange[EdgeProp], 4*4*4096)
			} else {
				g.MessageQ[i] = make(chan graph.Message, len(g.Vertices)+8)
			}
		}
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
	mathutils.BatchParallelFor(len(g.Vertices), graph.THREADS, func(idx int, tidx int) {
		frame.OnInitVertex(g, uint32(idx))
	})
	//t0 := time.Since(m0)
	//info("Initialized(ms) ", t0.Milliseconds())
}

func (frame *Framework[VertexProp, EdgeProp]) Run(g *graph.Graph[VertexProp, EdgeProp], inputWg *sync.WaitGroup, outputWg *sync.WaitGroup) {
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

func (frame *Framework[VertexProp, EdgeProp]) Launch(g *graph.Graph[VertexProp, EdgeProp], gName string, async bool, dynamic bool, oracle bool, undirected bool) {
	g.Undirected = undirected
	if !dynamic {
		g.LoadGraphStatic(gName, graph.EdgeParserFunc[EdgeProp](frame.EdgeParser))
	}

	frame.Init(g, async, dynamic)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	if dynamic {
		go g.LoadGraphDynamic(gName, graph.EdgeParserFunc[EdgeProp](frame.EdgeParser), &feederWg)
	}

	if oracle {
		exit := false
		defer func() { exit = true }()
		go frame.CompareToOracleRunnable(g, &exit, 1000*time.Millisecond)
	}

	frame.Run(g, &feederWg, &frameWait)
}

func (frame *Framework[VertexProp, EdgeProp]) CompareToOracleRunnable(g *graph.Graph[VertexProp, EdgeProp], exit *bool, sleepTime time.Duration) {
	time.Sleep(sleepTime)
	for !*exit {
		frame.CompareToOracle(g)
		time.Sleep(sleepTime)
	}
}

func (originalFrame *Framework[VertexProp, EdgeProp]) CompareToOracle(g *graph.Graph[VertexProp, EdgeProp]) {
	numEdges := uint64(0)

	g.Mutex.Lock()
	g.Watch.Pause()
	info("----INLINE----")
	info("inlineCurrentTime(ms) ", g.Watch.Elapsed().Milliseconds())
	newFrame := Framework[VertexProp, EdgeProp]{}
	newFrame.OnInitVertex = originalFrame.OnInitVertex
	newFrame.OnVisitVertex = originalFrame.OnVisitVertex
	newFrame.OnFinish = originalFrame.OnFinish
	newFrame.OnCheckCorrectness = originalFrame.OnCheckCorrectness
	newFrame.OnEdgeAdd = originalFrame.OnEdgeAdd
	newFrame.OnEdgeDel = originalFrame.OnEdgeDel
	newFrame.MessageAggregator = originalFrame.MessageAggregator
	newFrame.AggregateRetrieve = originalFrame.AggregateRetrieve

	altG := &graph.Graph[VertexProp, EdgeProp]{}
	altG.Undirected = g.Undirected
	altG.EmptyVal = g.EmptyVal
	altG.SourceInit = g.SourceInit
	altG.SourceInitVal = g.SourceInitVal
	altG.SourceVertex = g.SourceVertex

	altG.VertexMap = g.VertexMap // ok to shallow copy, we do not edit.
	altG.Vertices = make([]graph.Vertex[VertexProp, EdgeProp], len(g.Vertices))
	gVertexStash := make([]graph.Vertex[VertexProp, EdgeProp], len(g.Vertices))
	for v := range g.Vertices {
		altG.Vertices[v].Id = g.Vertices[v].Id
		altG.Vertices[v].OutEdges = g.Vertices[v].OutEdges
		numEdges += uint64(len(g.Vertices[v].OutEdges))
		gVertexStash[v].Id = g.Vertices[v].Id
		gVertexStash[v].Scratch = g.Vertices[v].Scratch
		gVertexStash[v].Property = g.Vertices[v].Property
	}

	if resultCache == nil {
		newFrame.Init(altG, true, false)
		var feederWg sync.WaitGroup
		feederWg.Add(1)
		var frameWait sync.WaitGroup
		frameWait.Add(1)
		newFrame.Run(altG, &feederWg, &frameWait)
	}

	// Here we "early finish" proper G immediately for a fair comparison (i.e., including sink adjustment)
	// to compare a fully finished to the current state. Since the OnFinish is minute in cost but big in effect,
	// important to compare with it applied to both .
	newFrame.OnFinish(g)

	originalFrame.OracleComparison(g, altG, &resultCache)

	for v := range g.Vertices {
		// Resetting the effect of the "early finish"
		g.Vertices[v].Scratch = gVertexStash[v].Scratch
		g.Vertices[v].Property = gVertexStash[v].Property
	}

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
