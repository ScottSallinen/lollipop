package framework

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
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

type Framework[VertexProp, EdgeProp, MsgType any] struct {
	OnInitVertex       OnInitVertexFunc[VertexProp, EdgeProp, MsgType]
	OnVisitVertex      OnVisitVertexFunc[VertexProp, EdgeProp, MsgType]
	OnFinish           OnFinishFunc[VertexProp, EdgeProp, MsgType]
	OnCheckCorrectness OnCheckCorrectnessFunc[VertexProp, EdgeProp, MsgType]
	OnEdgeAdd          OnEdgeAddFunc[VertexProp, EdgeProp, MsgType]
	OnEdgeDel          OnEdgeDelFunc[VertexProp, EdgeProp, MsgType]
	OnEdgeAddRev       OnEdgeAddRevFunc[VertexProp, EdgeProp, MsgType]
	OnEdgeDelRev       OnEdgeDelRevFunc[VertexProp, EdgeProp, MsgType]
	MessageAggregator  MessageAggregatorFunc[VertexProp, EdgeProp, MsgType]
	AggregateRetrieve  AggregateRetrieveFunc[VertexProp, EdgeProp, MsgType]
	OracleComparison   OracleComparison[VertexProp, EdgeProp, MsgType]
	EdgeParser         EdgeParserFunc[EdgeProp]
	IsMsgEmpty         func(MsgType) bool
}

type OnInitVertexFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], vidx uint32)
type OnVisitVertexFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], vidx uint32, data MsgType) int
type OnFinishFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType]) error
type OnCheckCorrectnessFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType]) error
type OnEdgeAddFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didxStart int, VisitMsg MsgType) (RevData []MsgType)
type OnEdgeDelFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitMsg MsgType) (RevData MsgType)
type OnEdgeAddRevFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didxStart int, SourceMsgs []MsgType)
type OnEdgeDelRevFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitMsg MsgType)
type MessageAggregatorFunc[VertexProp, EdgeProp, MsgType any] func(dst *graph.Vertex[VertexProp, EdgeProp], didx, sidx uint32, VisitMsg MsgType) (newInfo bool)
type AggregateRetrieveFunc[VertexProp, EdgeProp, MsgType any] func(g *graph.Vertex[VertexProp, EdgeProp]) (data MsgType)
type OracleComparison[VertexProp, EdgeProp, MsgType any] func(g *graph.Graph[VertexProp, EdgeProp, MsgType], oracle *graph.Graph[VertexProp, EdgeProp, MsgType], resultCache *[]float64)
type EdgeParserFunc[EdgeProp any] graph.EdgeParserFunc[EdgeProp]

func (frame *Framework[VertexProp, EdgeProp, MsgType]) Init(g *graph.Graph[VertexProp, EdgeProp, MsgType], async bool, dynamic bool) {
	//info("Started.")
	if async || dynamic {
		g.OnQueueVisit = frame.OnQueueVisitAsync
		if dynamic {
			g.VertexMap = make(map[uint32]uint32, 4*4096)
			if g.Undirected || g.SendRevMsgs {
				g.ReverseMsgQ = make([]chan graph.RevMessage[MsgType, EdgeProp], graph.THREADS)
			}
		}
		g.MessageQ = make([]chan graph.Message[MsgType], graph.THREADS)
		g.ThreadStructureQ = make([]chan graph.StructureChange[EdgeProp], graph.THREADS)
		g.MsgSend = make([]uint32, graph.THREADS+1)
		g.MsgRecv = make([]uint32, graph.THREADS+1)
		g.TerminateVote = make([]int, graph.THREADS+1)
		g.TerminateData = make([]int64, graph.THREADS+1)
		for i := 0; i < graph.THREADS; i++ {
			if dynamic {
				// TODO: Need a better way to manipulate channel size for dynamic. Maybe request approx vertex count from user?
				g.MessageQ[i] = make(chan graph.Message[MsgType], (4 * 4096 * 64))
				g.ThreadStructureQ[i] = make(chan graph.StructureChange[EdgeProp], 4*4*4096)
				if g.Undirected || g.SendRevMsgs {
					g.ReverseMsgQ[i] = make(chan graph.RevMessage[MsgType, EdgeProp], 4*4*4096)
				}
			} else {
				g.MessageQ[i] = make(chan graph.Message[MsgType], len(g.Vertices)+8)
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

func (frame *Framework[VertexProp, EdgeProp, MsgType]) Run(g *graph.Graph[VertexProp, EdgeProp, MsgType], inputWg *sync.WaitGroup, outputWg *sync.WaitGroup) {
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

func (frame *Framework[VertexProp, EdgeProp, MsgType]) Launch(g *graph.Graph[VertexProp, EdgeProp, MsgType], gName string, async bool, dynamic bool, oracle bool, undirected bool) {
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

func (frame *Framework[VertexProp, EdgeProp, MsgType]) CompareToOracleRunnable(g *graph.Graph[VertexProp, EdgeProp, MsgType], exit *bool, sleepTime time.Duration) {
	time.Sleep(sleepTime)
	for !*exit {
		frame.CompareToOracle(g)
		time.Sleep(sleepTime)
	}
}

func (originalFrame *Framework[VertexProp, EdgeProp, MsgType]) CompareToOracle(g *graph.Graph[VertexProp, EdgeProp, MsgType]) {
	numEdges := uint64(0)

	g.Mutex.Lock()
	g.Watch.Pause()
	info("----INLINE----")
	info("inlineCurrentTime(ms) ", g.Watch.Elapsed().Milliseconds())
	newFrame := Framework[VertexProp, EdgeProp, MsgType]{}
	newFrame.OnInitVertex = originalFrame.OnInitVertex
	newFrame.OnVisitVertex = originalFrame.OnVisitVertex
	newFrame.OnFinish = originalFrame.OnFinish
	newFrame.OnCheckCorrectness = originalFrame.OnCheckCorrectness
	newFrame.OnEdgeAdd = originalFrame.OnEdgeAdd
	newFrame.OnEdgeDel = originalFrame.OnEdgeDel
	newFrame.MessageAggregator = originalFrame.MessageAggregator
	newFrame.AggregateRetrieve = originalFrame.AggregateRetrieve
	newFrame.IsMsgEmpty = originalFrame.IsMsgEmpty

	altG := &graph.Graph[VertexProp, EdgeProp, MsgType]{}
	altG.Undirected = g.Undirected
	altG.EmptyVal = g.EmptyVal
	altG.SourceInit = g.SourceInit
	altG.InitVal = g.InitVal
	altG.SourceVertex = g.SourceVertex

	altG.VertexMap = g.VertexMap // ok to shallow copy, we do not edit.
	altG.Vertices = make([]graph.Vertex[VertexProp, EdgeProp], len(g.Vertices))
	gVertexStash := make([]graph.Vertex[VertexProp, EdgeProp], len(g.Vertices))
	for v := range g.Vertices {
		altG.Vertices[v].Id = g.Vertices[v].Id
		altG.Vertices[v].OutEdges = g.Vertices[v].OutEdges
		numEdges += uint64(len(g.Vertices[v].OutEdges))
		gVertexStash[v].Id = g.Vertices[v].Id
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

func ExtractGraphName(graphFilename string) (graphName string) {
	gNameMainT := strings.Split(graphFilename, "/")
	gNameMain := gNameMainT[len(gNameMainT)-1]
	gNameMainTD := strings.Split(gNameMain, ".")
	if len(gNameMainTD) > 1 {
		return gNameMainTD[len(gNameMainTD)-2]
	} else {
		return gNameMainTD[0]
	}
}

func ShuffleSC[EdgeProp any](sc []graph.StructureChange[EdgeProp]) {
	for i := range sc {
		j := rand.Intn(i + 1)
		sc[i], sc[j] = sc[j], sc[i]
	}
}

func InjectDeletesRetainFinalStructure[EdgeProp any](sc []graph.StructureChange[EdgeProp], chance float64) []graph.StructureChange[EdgeProp] {
	availableAdds := make([]graph.StructureChange[EdgeProp], len(sc))
	var previousAdds []graph.StructureChange[EdgeProp]
	var returnSC []graph.StructureChange[EdgeProp]

	copy(availableAdds, sc)
	ShuffleSC(availableAdds)

	for len(availableAdds) > 0 {
		if len(previousAdds) > 0 && rand.Float64() < chance {
			// chance for del
			ShuffleSC(previousAdds)
			idx := len(previousAdds) - 1
			injDel := graph.StructureChange[EdgeProp]{Type: graph.DEL, SrcRaw: previousAdds[idx].SrcRaw, DstRaw: previousAdds[idx].DstRaw, EdgeProperty: previousAdds[idx].EdgeProperty}
			returnSC = append(returnSC, injDel)
			availableAdds = append(availableAdds, previousAdds[idx])
			previousAdds = previousAdds[:idx]
		} else {
			ShuffleSC(availableAdds)
			idx := len(availableAdds) - 1
			returnSC = append(returnSC, availableAdds[idx])
			previousAdds = append(previousAdds, availableAdds[idx])
			availableAdds = availableAdds[:idx]
		}
	}
	return returnSC
}
