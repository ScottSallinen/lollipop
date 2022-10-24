package graph

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...any) {
	log.Println("[Graph]\t", fmt.Sprint(args...))
}

var THREADS = 32
var TARGETRATE = float64(0)
var DEBUG = false

const BIGNESS = 4 // Multiplier on elements like queue size and bundle size. e.g. suggest 64 for billion-edge graphs.
const MsgBundleSize = 256
const GscBundleSize = (1 << 12) * BIGNESS

// Graph t
type Graph[VertexProp, EdgeProp, MsgType any] struct {
	Mutex            sync.RWMutex
	VertexMap        map[uint32]uint32 // Raw to internal
	Vertices         []Vertex[VertexProp, EdgeProp]
	OnQueueVisit     func(g *Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitData MsgType)
	AlgConverge      func(g *Graph[VertexProp, EdgeProp, MsgType], wg *sync.WaitGroup)
	MessageQ         []chan Message[MsgType]
	ThreadStructureQ []chan StructureChange[EdgeProp]
	MsgSend          []uint32 // number of messages sent by each thread
	MsgRecv          []uint32 // number of messages received by each thread
	TerminateVote    []int
	TerminateData    []int64
	Watch            mathutils.Watch
	LogEntryChan     chan time.Time
	Options          GraphOptions[MsgType]
}

type GraphOptions[MsgType any] struct {
	Undirected            bool               // Declares if the graph should be treated as undirected (e.g. for construction)
	LogTimeseries         bool               // Uses timestamps to log a timeseries of vertex properties.
	TimeSeriesInterval    uint64             // Interval (seconds) for how often to log timeseries.
	OracleCompare         bool               // Will compare to computed oracle results, either from an interval or, if creating a timeseries, each time a timeseries is logged.
	EmptyVal              MsgType            // Value used to represent "empty" or "no work to do"
	SourceInit            bool               // If set to true, InitMessages defines how initial messages will be sent. Otherwise, all vertices will receive InitAllMessage.
	InitMessages          map[uint32]MsgType // Mapping from raw vertex ID to the initial messages they receive
	InitAllMessage        MsgType            // Value to initialize when SourceInit is set to false
	InsertDeleteOnExpire  uint64             // If non-zero, will insert delete edges that were added before, after passing the expiration duration. (Create a sliding window graph). Needs (Get/Set)Timestamp defined.
	OracleInterval        int64              // If OracleCompare, will compare to oracle results every OracleInterval milliseconds.
	AsyncContinuationTime int64              // If non-zero, will continue the algorithm for AsyncContinuationTime milliseconds before collecting a state (logging a timeseries).
	OracleCompareSync     bool               // Compares to oracle results on every iteration, when using a synchronous strategy.
}

type VisitType int

const (
	VISIT VisitType = iota
	ADD
	DEL
	VISITEMPTYMSG
)

type Message[MsgType any] struct {
	Message MsgType
	Type    VisitType
	Sidx    uint32
	Didx    uint32
}

type StructureChange[EdgeProp any] struct {
	Type         VisitType // add or del
	SrcRaw       uint32
	DstRaw       uint32
	EdgeProperty EdgeProp
}

type EdgeParserFunc[EdgeProp any] func(lineText string) RawEdge[EdgeProp]

// Edge: Basic edge type for a graph, with a user-defined property; can be empty struct{}
// Note the order of Property and Destination here matters. Currently, each Edge[struct{}] takes 4 bytes of
// memory, but if we re-order these two fields, the size becomes 8 bytes.
type Edge[EdgeProp any] struct {
	Property    EdgeProp
	Destination uint32
}

// InEdge: TODO, this is very outdated.
type InEdge struct {
	Destination uint32
	Weight      float64
}

// Vertex Main type defining a vertex in a graph. Contains necessary per-vertex information for structure and identification.
type Vertex[VertexProp, EdgeProp any] struct {
	Property VertexProp       // Generic property type, can be variable per algorithm.
	Id       uint32           // Raw (external) ID of a vertex, reflecting the external original identifier of a vertex, NOT the internal [0, N] index.
	OutEdges []Edge[EdgeProp] // Main outgoing edgelist.
	InEdges  []InEdge         // Incoming edges (currently unused).
	Mutex    sync.RWMutex     // Mutex for thread synchroniziation, if needed.
	IsActive int32            // Indicates if the vertex awaits a visit in ConvergeSync
}

func (v *Vertex[VertexProp, EdgeProp]) ToThreadIdx() uint32 {
	return v.Id % uint32(THREADS)
}

func (g *Graph[VertexProp, EdgeProp, MsgType]) RawIdToThreadIdx(RawId uint32) uint32 {
	return RawId % uint32(THREADS)
}

// ComputeInEdges update all vertices' InEdges to match the edges stored in OutEdges lists
func (g *Graph[VertexProp, EdgeProp, MsgType]) ComputeInEdges() {
	for vidx := range g.Vertices {
		for eidx := range g.Vertices[vidx].OutEdges {
			target := int(g.Vertices[vidx].OutEdges[eidx].Destination)
			g.Vertices[target].InEdges = append(g.Vertices[target].InEdges, InEdge{uint32(vidx), 0.0})
		}
	}
	info("Computed inbound edges.")
}

// ComputeGraphStats prints some statistics of the graph
func (g *Graph[VertexProp, EdgeProp, MsgType]) ComputeGraphStats(inDeg bool, outDeg bool) {
	maxOutDegree := uint64(0)
	maxInDegree := uint64(0)
	listInDegree := []int{}
	listOutDegree := []int{}
	numSinks := uint64(0)
	numEdges := uint64(0)

	for vidx := range g.Vertices {
		if len(g.Vertices[vidx].OutEdges) == 0 {
			numSinks++
		}
		numEdges += uint64(len(g.Vertices[vidx].OutEdges))
		if outDeg {
			maxOutDegree = mathutils.Max(uint64(len(g.Vertices[vidx].OutEdges)), maxOutDegree)
			listOutDegree = append(listOutDegree, len(g.Vertices[vidx].OutEdges))
		}
		if inDeg {
			maxInDegree = mathutils.Max(uint64(len(g.Vertices[vidx].InEdges)), maxInDegree)
			listInDegree = append(listInDegree, len(g.Vertices[vidx].InEdges))
		}
	}

	info("----GraphStats----")
	info("Vertices ", len(g.Vertices))
	info("Sinks ", numSinks, " pct:", fmt.Sprintf("%.3f", float64(numSinks)*100.0/float64(len(g.Vertices))))
	info("Edges ", numEdges)
	if outDeg {
		info("MaxOutDeg ", maxOutDegree)
		info("MedianOutDeg ", mathutils.Median(listOutDegree))
	}
	if inDeg {
		info("MaxInDeg ", maxInDegree)
		info("MedianInDeg ", mathutils.Median(listInDegree))
	}
	info("----EndStats----")
}

// Compares two arrays: showcases average and L1 differences.
// IgnoreSize: some number to ignore when computing 95th percentile. (i.e., for ignoring singletons)
// Returns: (Average L1 diff, 95th percentile L1 diff, Largest Absolute percent diff)
func ResultCompare(a []float64, b []float64, ignoreSize int) (float64, float64, float64) {
	largestRelativeDiff := float64(0)
	avgRelativeDiff := float64(0)
	listDiff := []float64{}
	largestL1Diff := float64(0)
	avgL1Diff := float64(0)
	listL1Diff := []float64{}

	for idx := range a {
		delta := math.Abs((b[idx] - a[idx]) * 100.0 / math.Min(a[idx], b[idx]))
		listDiff = append(listDiff, delta)
		avgRelativeDiff += delta
		largestRelativeDiff = mathutils.Max(largestRelativeDiff, delta)

		l1delta := math.Abs(b[idx] - a[idx])
		listL1Diff = append(listL1Diff, l1delta)
		avgL1Diff += l1delta
		largestL1Diff = mathutils.Max(largestL1Diff, l1delta)
	}
	avgRelativeDiff = avgRelativeDiff / float64(len(a))
	avgL1Diff = avgL1Diff / float64(len(a))

	sort.Float64s(listDiff)
	sort.Float64s(listL1Diff)

	medianIdx := len(listDiff) / 2
	medianDiff := listDiff[medianIdx]
	medianL1Diff := listL1Diff[medianIdx]
	if len(listDiff)%2 == 1 { // odd
		medianDiff = (listDiff[medianIdx-1] + listDiff[medianIdx]) / 2
		medianL1Diff = (listL1Diff[medianIdx-1] + listL1Diff[medianIdx]) / 2
	}
	percentile95 := listDiff[int(float64(len(listDiff))*0.95)]
	//percentile95L1 := listL1Diff[int(float64(len(listDiff))*0.95)]
	percentile95L1 := listL1Diff[int(float64(len(listDiff)-ignoreSize)*0.95)+ignoreSize]

	info(fmt.Sprintf("AverageRel %.5f MedianRel %.5f 95pRel %.5f LargestRel %.5f", avgRelativeDiff, medianDiff, percentile95, largestRelativeDiff))
	info(fmt.Sprintf("AverageL1 %.3e MedianL1 %.3e 95pL1 %.3e LargestL1 %.3e", avgL1Diff, medianL1Diff, percentile95L1, largestL1Diff))
	return avgL1Diff, percentile95L1, largestRelativeDiff
}

func (g *Graph[VertexProp, EdgeProp, MsgType]) PrintStructure() {
	log.Println(g.VertexMap)
	for vidx := range g.Vertices {
		pr := fmt.Sprintf("%d", g.Vertices[vidx].Id)
		el := ""
		for _, e := range g.Vertices[vidx].OutEdges {
			el += fmt.Sprintf("%d, ", g.Vertices[e.Destination].Id)
		}
		log.Println(pr + ": " + el)
	}
}

func (g *Graph[VertexProp, EdgeProp, MsgType]) PrintVertexProperty(prefix string) {
	message := prefix
	for vi := range g.Vertices {
		message += fmt.Sprintf("%d:%v, ", g.Vertices[vi].Id, &g.Vertices[vi].Property)
	}
	info(message)
}

func (g *Graph[VertexProp, EdgeProp, MsgType]) PrintVertexInEdgeSum(prefix string) {
	top := prefix
	sum := 0.0
	for vidx := range g.Vertices {
		localsum := 0.0
		for eidx := range g.Vertices[vidx].InEdges {
			localsum += g.Vertices[vidx].InEdges[eidx].Weight
		}
		sum += localsum
		top += fmt.Sprintf("%.3f", localsum) + " "
	}
	info(top + " : " + fmt.Sprintf("%.3f", sum))
}

func (g *Graph[VertexProp, EdgeProp, MsgType]) WriteVertexProps(graphName string, dynamic bool) {
	var resName string
	if dynamic {
		resName = "dynamic"
	} else {
		resName = "static"
	}
	filename := "results/" + graphName + "-props-" + resName + ".txt"

	f, err := os.Create(filename)
	enforce.ENFORCE(err)
	defer f.Close()

	for vidx := range g.Vertices {
		_, err := f.WriteString(fmt.Sprintf("%d - %v\n", g.Vertices[vidx].Id, &g.Vertices[vidx].Property))
		enforce.ENFORCE(err)
	}
}
