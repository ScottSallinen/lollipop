package main

import (
	"flag"
	"fmt"
	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/framework"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var tsDB = make([]framework.TimeseriesEntry[VertexProp], 0)

func info(args ...any) {
	log.Println("[MaxFlowNew]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProp] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 3 || sflen == 4)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])
	capacity, _ := strconv.Atoi(stringFields[2])
	timestamp := 0
	if sflen == 4 {
		timestamp, _ = strconv.Atoi(stringFields[3])
	}

	return graph.RawEdge[EdgeProp]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProp{
		Capacity:  uint32(capacity),
		Timestamp: uint64(timestamp),
	}}
}

func OnCheckCorrectness(g *Graph, sourceRaw, sinkRaw uint32) error {
	source := &g.Vertices[g.VertexMap[sourceRaw]]
	sink := &g.Vertices[g.VertexMap[sinkRaw]]
	enforce.ENFORCE(source.Property.Type == Source)
	enforce.ENFORCE(sink.Property.Type == Sink)

	// Make sure all messages are processed
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		enforce.ENFORCE(len(v.Property.MessageBuffer) == 0, fmt.Sprintf("vertex index %d ID %d has outstanding messages", vi, v.Id))
	}

	// Check heights
	enforce.ENFORCE(source.Property.Height >= int64(len(g.Vertices)),
		"source height ", source.Property.Height, " < # of vertices ", len(g.Vertices))
	enforce.ENFORCE(sink.Property.Height == 0, "sink height != 0")

	// Check Excess
	for vi := range g.Vertices {
		if v := &g.Vertices[vi]; v.Property.Type == Normal {
			enforce.ENFORCE(v.Property.Excess == 0, fmt.Sprintf("normal vertex index %d ID %d has a non-zero excess of %d", vi, v.Id, v.Property.Excess))
		}
	}

	// Check sum of edge capacities in the original graph == in the residual graph
	for vi := range g.Vertices {
		if v := &g.Vertices[vi]; v.Property.Type == Normal {
			sumEdgeCapacityOriginal := int64(0)
			sumEdgeCapacityResidual := int64(0)
			for ei := range v.OutEdges {
				e := &v.OutEdges[ei]
				// ignore loops and edges to the source
				if e.Destination != uint32(vi) && e.Destination != g.VertexMap[sourceRaw] {
					sumEdgeCapacityOriginal += int64(v.OutEdges[ei].Property.Capacity)
				}
			}
			for _, neighbour := range v.Property.Nbrs {
				sumEdgeCapacityResidual += neighbour.ResCap
			}
			enforce.ENFORCE(sumEdgeCapacityOriginal == sumEdgeCapacityResidual, fmt.Sprintf(
				"normal vertex index %d ID %d sumEdgeCapacityOriginal (%d) != sumEdgeCapacityResidual (%d)",
				vi, v.Id, sumEdgeCapacityOriginal, sumEdgeCapacityResidual),
			)
		}
	}

	// Check sourceOut and sinkIn
	sinkIn := sink.Property.Excess
	sourceOut := int64(0)
	for ei := range source.OutEdges {
		edge := &source.OutEdges[ei]
		sourceOut += int64(edge.Property.Capacity)
	}
	sourceOut -= source.Property.Excess
	enforce.ENFORCE(sourceOut == sinkIn, fmt.Sprintf("sourceOutFlow (%d) != sinkInFlow (%d)", sourceOut, sinkIn))
	info("Maximum Flow: ", sourceOut)

	// g.ComputeInEdges()
	// TODO: Check inflow == outflow for all vertices (doesn't seem to be easy)
	// TODO: Check height invariant
	// TODO: print # of vertices in flow

	PrintMessageCounts()
	ResetMessageCounts()
	return nil
}

func GetFrameworkAndGraph(sourceRaw, sinkRaw, n uint32, exit *chan bool, insertDeleteDelay uint64, timeSeriesInterval uint64) (*Framework, *Graph) {
	enforce.ENFORCE(sourceRaw != sinkRaw)

	g := Graph{}
	g.Options = graph.GraphOptions[MessageValue]{
		Undirected:           false,
		EmptyVal:             nil,
		LogTimeseries:        timeSeriesInterval != 0,
		OracleCompare:        false,
		SourceInit:           false,
		ReadLockRequired:     true,
		InsertDeleteOnExpire: insertDeleteDelay,
		TimeSeriesInterval:   timeSeriesInterval,
		InitAllMessage: MessageValue{{
			Type:   Init,
			Source: EmptyValue,
			Height: EmptyValue,
			Value:  EmptyValue,
		}},
	}

	frame := Framework{}
	frame.OnVisitVertex = OnVisitVertex
	frame.OnFinish = func(g *Graph) error {
		if g.Options.LogTimeseries {
			return nil
		}
		return OnFinish(g, exit)
	}
	frame.OnEdgeAdd = OnEdgeAdd
	frame.OnEdgeDel = OnEdgeDel
	frame.MessageAggregator = MessageAggregator
	frame.AggregateRetrieve = AggregateRetrieve
	frame.EdgeParser = EdgeParser
	frame.GetTimestamp = GetTimestamp
	frame.SetTimestamp = SetTimestamp
	frame.ApplyTimeSeries = ApplyTimeSeries
	frame.NewLogTimeSeries = func(f *Framework, g *Graph, entries chan framework.TimeseriesEntry[VertexProp]) {
		LogTimeSeries(f, g, entries, sourceRaw, sinkRaw)
	}
	frame.OnInitVertex = func(g *Graph, vidx uint32) {
		v := &g.Vertices[vidx]
		switch v.Id {
		case sourceRaw:
			v.Property.Type = Source
			v.Property.Height = 0
		case sinkRaw:
			v.Property.Type = Sink
			v.Property.Height = 0
		default:
			v.Property.Type = Normal
			v.Property.Height = math.MaxUint32 // OPTIMIZATION
		}
		v.Property.Nbrs = make(map[uint32]Nbr)
		v.Property.Excess = 0
	}
	frame.OnCheckCorrectness = func(g *Graph) error {
		return OnCheckCorrectness(g, sourceRaw, sinkRaw)
	}

	VertexCountHelper.Reset(int64(n))

	return &frame, &g
}

func LaunchGraphExecution(gName string, async bool, dynamic bool, source, sink, n uint32, grInterval time.Duration, insertDeleteDelay uint64, timeSeriesInterval uint64) *Graph {
	enforce.ENFORCE(async || dynamic, "Max flow currently does not support sync")
	globalRelabelExit := make(chan bool, 0)
	frame, g := GetFrameworkAndGraph(source, sink, n, &globalRelabelExit, insertDeleteDelay, timeSeriesInterval)
	Launch(frame, g, gName, async, dynamic, grInterval, &globalRelabelExit)
	return g
}

func Launch(f *Framework, g *Graph, gName string, async bool, dynamic bool, grInterval time.Duration, grExit *chan bool) {
	if !dynamic {
		g.LoadGraphStatic(gName, f.EdgeParser)
	}

	f.Init(g, async, dynamic)

	var feederWg sync.WaitGroup
	feederWg.Add(1)
	var frameWait sync.WaitGroup
	frameWait.Add(1)

	var grWg *sync.WaitGroup
	if async {
		if !g.Options.LogTimeseries {
			grWg = StartPeriodicGlobalReset(f, g, grInterval, grInterval, true, grExit)
		}
	} else {
		enforce.ENFORCE("Global Reset currently does not work in sync mode")
	}

	if dynamic {
		go g.LoadGraphDynamic(gName, f.EdgeParser, &feederWg)
	}

	// Only if we're not logging a timeseries, we launch an interval oracle comparer
	if g.Options.OracleCompare && !g.Options.LogTimeseries {
		exit := false
		defer func() { exit = true }()
		go f.CompareToOracleRunnable(g, &exit, time.Duration(g.Options.OracleInterval))
	}

	// We launch the timeseries consumer thread
	if g.Options.LogTimeseries {
		entries := make(chan framework.TimeseriesEntry[VertexProp], 4096)
		go f.NewLogTimeSeries(f, g, entries)
		go f.ApplyTimeSeries(entries)
	}

	f.Run(g, &feederWg, &frameWait)

	if !g.Options.LogTimeseries {
		grWg.Wait()
	}
}

func main() {
	gptr := flag.String("g", "data/maxflow/test-1.txt", "Graph file")
	aptr := flag.Bool("a", false, "Use async")
	dptr := flag.Bool("d", false, "Dynamic")
	rptr := flag.Float64("r", 0, "Use Dynamic Rate, with given rate in Edge Per Second. 0 is unbounded.")
	pptr := flag.Bool("p", false, "Save vertex properties to disk")
	tptr := flag.Int("t", 32, "Thread count")
	source := flag.Uint("source", 0, "Raw ID of the source vertex")
	sink := flag.Uint("sink", 1, "Raw ID of the sink vertex")
	n := flag.Uint("n", 0, "Number of vertices in the graph")
	insertDeleteDelay := flag.Uint("sw", 0,
		"If non-zero, will insert delete edges that were added before, after passing the expiration duration")
	tsi := flag.Uint("tsi", 0, "Timeseries interval")
	flag.Parse()

	graph.THREADS = *tptr
	graph.TARGETRATE = *rptr

	//runtime.SetMutexProfileFraction(1)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	g := LaunchGraphExecution(*gptr, *aptr, *dptr, uint32(*source), uint32(*sink), uint32(*n),
		10*time.Second, uint64(*insertDeleteDelay), uint64(*tsi))

	g.ComputeGraphStats(false, false)

	if *pptr {
		graphName := framework.ExtractGraphName(*gptr)
		g.WriteVertexProps(graphName, *dptr)
	}
}

func LogTimeSeries(f *Framework, g *Graph, entries chan framework.TimeseriesEntry[VertexProp], sourceRaw, sinkRaw uint32) {
	for entry := range g.LogEntryChan {
		g.Mutex.Lock()

		// Count positiveVertices and negativeVertices
		g.Watch.Pause()
		positiveVertices := uint32(0)
		negativeVertices := uint32(0)
		for vi := range g.Vertices {
			v := &g.Vertices[vi]
			if v.Property.Excess > 0 {
				positiveVertices += 1
			} else if v.Property.Excess < 0 {
				negativeVertices += 1
			}
		}
		g.Watch.UnPause()

		latencyWatch := mathutils.Watch{}
		latencyWatch.Start()

		SourceApproxMaxFlow := int64(0)
		SinkApproxMaxFlow := int64(0)

		// Before
		sourceIdx, hasSource := g.VertexMap[sourceRaw]
		sinkIdx, hasSink := g.VertexMap[sinkRaw]
		if hasSource && hasSink {
			source := &g.Vertices[sourceIdx]
			sink := &g.Vertices[sinkIdx]
			SinkApproxMaxFlow = sink.Property.Excess
			for ei := range source.OutEdges {
				edge := &source.OutEdges[ei]
				SourceApproxMaxFlow += int64(edge.Property.Capacity)
			}
			SourceApproxMaxFlow -= source.Property.Excess
		}

		// Run until termination
		done := f.ProcessAllMessagesWithTimeout(g, 1000*time.Millisecond)
		for !done {
			GlobalRelabel(f, g, false)
			done = f.ProcessAllMessagesWithTimeout(g, 1000*time.Millisecond)
		}
		latency := latencyWatch.Elapsed()

		// Get Max Flow
		FinalMaxFlow := int64(0)
		if hasSource && hasSink {
			FinalMaxFlow = g.Vertices[sinkIdx].Property.Excess
		}

		// Save ts entry
		g.Watch.Pause()
		if hasSource && hasSink {
			enforce.ENFORCE(OnCheckCorrectness(g, sourceRaw, sinkRaw))
		}
		numEdges := 0
		for vi := range g.Vertices {
			numEdges += len(g.Vertices[vi].OutEdges)
		}
		tsEntry := framework.TimeseriesEntry[VertexProp]{
			Name:                entry,
			VertexCount:         len(g.Vertices),
			EdgeCount:           uint64(numEdges),
			PositiveVertices:    positiveVertices,
			NegativeVertices:    negativeVertices,
			SourceApproxMaxFlow: SourceApproxMaxFlow,
			SinkApproxMaxFlow:   SinkApproxMaxFlow,
			FinalMaxFlow:        FinalMaxFlow,
			Latency:             latency,
		}
		tsDB = append(tsDB, tsEntry)
		entries <- tsEntry
		g.Watch.UnPause()

		g.ResetVotes()
		g.Mutex.Unlock()
	}
	close(entries)
}

func ApplyTimeSeries(entries chan framework.TimeseriesEntry[VertexProp]) {
	for e := range entries {
		info(tsEntryLineToStr(&e))
	}
	PrintTimeSeriesStat()
	SaveTimeSeries()
}

func SaveTimeSeries() {
	f, err := os.Create("timeseries.txt")
	enforce.ENFORCE(err)
	defer enforce.Close(f)

	header := "RFC3339,Date,VertexCount,EdgeCount,PositiveVertices,NegativeVertices," +
		"SourceApproxMaxFlow,SinkApproxMaxFlow,FinalMaxFlow,Latency,"
	f.WriteString(header + "\n")

	for i := range tsDB {
		line := tsEntryLineToStr(&tsDB[i])
		f.WriteString(line + "\n")
	}
}

func PrintTimeSeriesStat() {
	latencies := make([]int, len(tsDB))
	for i := range tsDB {
		latencies[i] = int(tsDB[i].Latency.Milliseconds())
	}
	mean := float64(mathutils.Sum(latencies)) / float64(len(latencies))
	median := mathutils.Median(latencies)
	min, max := mathutils.MinMax(latencies)
	variance := mathutils.SampleVariance(latencies, mean)
	sd := math.Sqrt(variance)
	info("Latency min=", min, " max=", max, " mean=", mean, " median=", median, " sd=", sd)
}

func tsEntryLineToStr(entry *framework.TimeseriesEntry[VertexProp]) string {
	return entry.Name.Format(time.RFC3339) + "," + entry.Name.Format("2006-01-02") + "," +
		strconv.FormatInt(int64(entry.VertexCount), 10) + "," +
		strconv.FormatUint(entry.EdgeCount, 10) + "," +
		strconv.FormatUint(uint64(entry.PositiveVertices), 10) + "," +
		strconv.FormatUint(uint64(entry.NegativeVertices), 10) + "," +
		strconv.FormatInt(entry.SourceApproxMaxFlow, 10) + "," +
		strconv.FormatInt(entry.SinkApproxMaxFlow, 10) + "," +
		strconv.FormatInt(entry.FinalMaxFlow, 10) + "," +
		strconv.FormatInt(entry.Latency.Milliseconds(), 10) + ","
}
