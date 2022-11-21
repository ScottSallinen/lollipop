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
	OnInitVertex       func(g *graph.Graph[VertexProp, EdgeProp, MsgType], vidx uint32)
	OnVisitVertex      func(g *graph.Graph[VertexProp, EdgeProp, MsgType], vidx uint32, data MsgType) int
	OnFinish           func(g *graph.Graph[VertexProp, EdgeProp, MsgType]) error
	OnCheckCorrectness func(g *graph.Graph[VertexProp, EdgeProp, MsgType]) error
	OnEdgeAdd          func(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didxStart int, VisitData MsgType)
	OnEdgeDel          func(g *graph.Graph[VertexProp, EdgeProp, MsgType], sidx uint32, deletedEdges []graph.Edge[EdgeProp], VisitData MsgType)
	MessageAggregator  func(dst *graph.Vertex[VertexProp, EdgeProp], didx, sidx uint32, VisitData MsgType) (newInfo bool)
	AggregateRetrieve  func(dst *graph.Vertex[VertexProp, EdgeProp]) (data MsgType)
	OracleComparison   func(g *graph.Graph[VertexProp, EdgeProp, MsgType], oracle *graph.Graph[VertexProp, EdgeProp, MsgType], resultCache *[]float64, cache bool)
	EdgeParser         graph.EdgeParserFunc[EdgeProp]
	GetTimestamp       func(prop EdgeProp) uint64
	SetTimestamp       func(prop *EdgeProp, ts uint64)
	ApplyTimeSeries    func(chan TimeseriesEntry[VertexProp])
}

type TimeseriesEntry[VertexProp any] struct {
	Name       time.Time
	EdgeCount  uint64
	VertexData []mathutils.Pair[uint32, VertexProp]
}

func (frame *Framework[VertexProp, EdgeProp, MsgType]) Init(g *graph.Graph[VertexProp, EdgeProp, MsgType], async bool, dynamic bool) {
	if async || dynamic {
		g.OnQueueVisit = frame.OnQueueVisitAsync
		if dynamic {
			g.VertexMap = make(map[uint32]uint32, ((1 << 12) * graph.BIGNESS))
		}
		g.MessageQ = make([]chan graph.Message[MsgType], graph.THREADS)
		g.ThreadStructureQ = make([]chan graph.StructureChange[EdgeProp], graph.THREADS)
		g.MsgSend = make([]uint32, graph.THREADS+1)
		g.MsgRecv = make([]uint32, graph.THREADS+1)
		g.TerminateVote = make([]int, graph.THREADS+1)
		g.TerminateData = make([]int64, graph.THREADS+1)
		g.LogEntryChan = make(chan time.Time, 512)
		for i := 0; i < graph.THREADS; i++ {
			if dynamic {
				g.MessageQ[i] = make(chan graph.Message[MsgType], ((1 << 19) * graph.BIGNESS))
				g.ThreadStructureQ[i] = make(chan graph.StructureChange[EdgeProp], ((1 << 14) * graph.BIGNESS))
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

	outputWg.Done()
	if g.LogEntryChan != nil {
		close(g.LogEntryChan)
	}
}

func (frame *Framework[VertexProp, EdgeProp, MsgType]) Launch(g *graph.Graph[VertexProp, EdgeProp, MsgType], gName string, async bool, dynamic bool) {
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

	// Only if we're not logging a timeseries, we launch an interval oracle comparer
	if g.Options.OracleCompare && !g.Options.LogTimeseries {
		exit := false
		defer func() { exit = true }()
		go frame.CompareToOracleRunnable(g, &exit, time.Duration(g.Options.OracleInterval))
	}

	// We launch the timeseries consumer thread
	if g.Options.LogTimeseries {
		entries := make(chan TimeseriesEntry[VertexProp], 4096)
		go frame.LogTimeSeriesRunnable(g, entries)
		go frame.ApplyTimeSeries(entries)
	}

	frame.Run(g, &feederWg, &frameWait)
}

func (frame *Framework[VertexProp, EdgeProp, MsgType]) CompareToOracleRunnable(g *graph.Graph[VertexProp, EdgeProp, MsgType], exit *bool, sleepTime time.Duration) {
	time.Sleep(sleepTime)
	for !*exit {
		frame.CompareToOracle(g, true, false, sleepTime)
	}
}

func (originalFrame *Framework[VertexProp, EdgeProp, MsgType]) CompareToOracle(g *graph.Graph[VertexProp, EdgeProp, MsgType], finishOriginal bool, cache bool, delay time.Duration) {
	numEdges := uint64(0)

	if delay > 0 {
		time.Sleep(delay * time.Millisecond)
	}

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

	altG := &graph.Graph[VertexProp, EdgeProp, MsgType]{}
	altG.Options = g.Options
	altG.Options.OracleCompare = false
	altG.Options.OracleCompareSync = false
	altG.Options.LogTimeseries = false

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

	if len(resultCache) == 0 {
		info("Creating result cache")
		newFrame.Init(altG, true, false)
		var feederWg sync.WaitGroup
		feederWg.Add(1)
		var frameWait sync.WaitGroup
		frameWait.Add(1)
		newFrame.Run(altG, &feederWg, &frameWait)
	}

	// Here we "early finish" proper G immediately for a fair comparison (i.e., including sink adjustment)
	// to compare a fully finished to the current state. Since the OnFinish is minute in cost but big in effect,
	// important to compare with it applied to both.
	if finishOriginal {
		originalFrame.OnFinish(g)
	}

	originalFrame.OracleComparison(g, altG, &resultCache, cache)

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

	g.Watch.UnPause()
	info("----END_INLINE----")
	g.Mutex.Unlock()
}

// LogTimeSeriesRunnable will check for any entry in the LogEntryChan. When on is supplied, it will
// immediately freeze the graph, and copy vertex properties. Then call "Finish" for the algorithm,
// then stash the finished vertex properties. Will then put back the original state of vertex properties.
// The stashed finished properties are then sent to the applyTimeSeries func (defined by the algorithm).
func (frame *Framework[VertexProp, EdgeProp, MsgType]) LogTimeSeriesRunnable(g *graph.Graph[VertexProp, EdgeProp, MsgType], entries chan TimeseriesEntry[VertexProp]) {
	for entry := range g.LogEntryChan {
		g.Mutex.Lock()

		// Shall be included in the runtime
		if g.Options.AsyncContinuationTime > 0 {
			wg := sync.WaitGroup{}
			gExit := false
			wg.Add(graph.THREADS)
			for t := 0; t < graph.THREADS; t++ {
				go func(tidx uint32, exit *bool) {
					msgBuffer := make([]graph.Message[MsgType], graph.MsgBundleSize)
					for !(*exit) {
						frame.ProcessMessages(g, tidx, msgBuffer, false, false)
					}
					wg.Done()
				}(uint32(t), &gExit)
			}
			time.Sleep(time.Duration(g.Options.AsyncContinuationTime) * time.Millisecond)
			gExit = true
			wg.Wait()
		}

		g.Watch.Pause()

		numEdges := uint64(0)
		gVertexStash := make([]mathutils.Pair[uint32, VertexProp], len(g.Vertices))
		for v := range g.Vertices {
			numEdges += uint64(len(g.Vertices[v].OutEdges))
			gVertexStash[v].First = g.Vertices[v].Id
			gVertexStash[v].Second = g.Vertices[v].Property
		}
		frame.OnFinish(g)

		for v := range g.Vertices {
			// Resetting the effect of the "early finish"
			tmp := g.Vertices[v].Property
			g.Vertices[v].Property = gVertexStash[v].Second
			gVertexStash[v].Second = tmp
		}
		entries <- TimeseriesEntry[VertexProp]{entry, numEdges, gVertexStash}
		g.Watch.UnPause()
		g.Mutex.Unlock()

		if g.Options.OracleCompare {
			frame.CompareToOracle(g, true, false, 0)
		}
	}
	close(entries)
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
		var current graph.StructureChange[EdgeProp]
		if len(previousAdds) > 0 && rand.Float64() < chance {
			current, previousAdds = mathutils.RemoveRandomElement(previousAdds)
			availableAdds = append(availableAdds, current)
			current.Type = graph.DEL
		} else {
			current, availableAdds = mathutils.RemoveRandomElement(availableAdds)
			previousAdds = append(previousAdds, current)
		}
		returnSC = append(returnSC, current)
	}
	return returnSC
}
