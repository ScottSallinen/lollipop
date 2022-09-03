package graph

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...any) {
	log.Println("[Graph]\t", fmt.Sprint(args...))
}

var THREADS = 32
var TARGETRATE = float64(0)
var DEBUG = false

// Graph t
type Graph[VertexProp, EdgeProp, MsgType any] struct {
	Mutex            sync.RWMutex
	VertexMap        map[uint32]uint32 // Raw to internal
	Vertices         []Vertex[VertexProp, EdgeProp]
	OnQueueVisit     OnQueueVisitFunc[VertexProp, EdgeProp, MsgType]
	AlgConverge      ConvergeFunc[VertexProp, EdgeProp, MsgType]
	MessageQ         []chan Message[MsgType]
	ThreadStructureQ []chan StructureChange[EdgeProp]
	ReverseMsgQ      []chan RevMessage[MsgType, EdgeProp]
	Undirected       bool     // Declares if the graph should be treated as undirected (e.g. for construction)
	SendRevMsgs      bool     // Whether or not to send EdgeAddRev / EdgeDelRev messages for the algorithm to view/handle. Effectively true if undirected.
	MsgSend          []uint32 // number of messages sent by each thread
	MsgRecv          []uint32 // number of messages received by each thread
	TerminateVote    []int
	TerminateData    []int64
	Watch            mathutils.Watch
	EmptyVal         MsgType // Value used to represent "empty" or "no work to do"
	InitVal          MsgType // Value to initialize, given either to single source (if SourceInit) or all vertices.
	SourceInit       bool    // Flag to adjust such that a single specific source vertex starts the algorithm, and will recieve InitVal.
	SourceVertex     uint32  // Raw ID of source vertex, if applicable.
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

type RevMessage[MsgType, EdgeProp any] struct {
	Message      MsgType   // From sender
	EdgeProperty EdgeProp  // Same as senders... tbd about sharing
	Type         VisitType // add or del
	Sidx         uint32
	Didx         uint32
}

type OnQueueVisitFunc[VertexProp, EdgeProp, MsgType any] func(g *Graph[VertexProp, EdgeProp, MsgType], sidx uint32, didx uint32, VisitData MsgType)
type ConvergeFunc[VertexProp, EdgeProp, MsgType any] func(g *Graph[VertexProp, EdgeProp, MsgType], wg *sync.WaitGroup)
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
	Mutex    sync.Mutex       // Mutex for thread synchroniziation, if needed.
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
			maxOutDegree = mathutils.MaxUint64(uint64(len(g.Vertices[vidx].OutEdges)), maxOutDegree)
			listOutDegree = append(listOutDegree, len(g.Vertices[vidx].OutEdges))
		}
		if inDeg {
			maxInDegree = mathutils.MaxUint64(uint64(len(g.Vertices[vidx].InEdges)), maxInDegree)
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

func ResultCompare(a []float64, b []float64) float64 {
	largestDiff := float64(0)
	smallestDiff := float64(0)
	avgDiff := float64(0)
	listDiff := []float64{}

	for idx := range a {
		delta := math.Abs((b[idx] - a[idx]) * 100.0 / math.Min(a[idx], b[idx]))
		listDiff = append(listDiff, delta)
		avgDiff += delta
		largestDiff = mathutils.MaxFloat64(largestDiff, delta)
		smallestDiff = mathutils.MinFloat64(smallestDiff, delta)
	}
	avgDiff = avgDiff / float64(len(a))

	sort.Float64s(listDiff)

	medianIdx := len(listDiff) / 2
	medianDiff := listDiff[medianIdx]
	if len(listDiff)%2 == 1 { // odd
		medianDiff = (listDiff[medianIdx-1] + listDiff[medianIdx]) / 2
	}
	percentile95 := listDiff[int(float64(len(listDiff))*0.95)]

	info("Average ", avgDiff, " Median ", medianDiff, " 95p ", percentile95, " Largest ", largestDiff)
	return largestDiff
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
