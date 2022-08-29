package graph

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"

	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...any) {
	log.Println("[Graph]\t", fmt.Sprint(args...))
}

var THREADS = 32
var TARGETRATE = float64(0)
var DEBUG = false

// Graph t
type Graph[VertexProp, EdgeProp any] struct {
	Mutex             sync.RWMutex
	VertexMap         map[uint32]uint32 // Raw to internal
	Vertices          []Vertex[VertexProp, EdgeProp]
	OnQueueVisit      OnQueueVisitFunc[VertexProp, EdgeProp]
	OnQueueEdgeAddRev OnQueueEdgeAddRevFunc[VertexProp, EdgeProp]
	AlgConverge       ConvergeFunc[VertexProp, EdgeProp]
	MessageQ          []chan Message
	ThreadStructureQ  []chan StructureChange[EdgeProp]
	MsgSend           []uint32 // number of messages sent by each thread
	MsgRecv           []uint32 // number of messages received by each thread
	TerminateVote     []int
	TerminateData     []int64
	Watch             mathutils.Watch
	EmptyVal          float64 // Value used to represent "empty" or "no work to do"
	SourceInitVal     float64 // Value to begin at source vertex
	SourceInit        bool    // A specific source vertex starts the algorithm.
	SourceVertex      uint32  // Raw ID of source vertex, if applicable.
}

type VisitType int

const (
	VISIT VisitType = iota
	ADD
	ADDREV
	DEL
	DELREV
)

type Message struct {
	Type VisitType
	Sidx uint32
	Didx uint32
	Val  float64
}

type StructureChange[EdgeProp any] struct {
	Type         VisitType // add or del
	SrcRaw       uint32
	DstRaw       uint32
	EdgeProperty EdgeProp
}

type OnQueueVisitFunc[VertexProp, EdgeProp any] func(g *Graph[VertexProp, EdgeProp], sidx uint32, didx uint32, VisitData float64)
type OnQueueEdgeAddRevFunc[VertexProp, EdgeProp any] func(g *Graph[VertexProp, EdgeProp], sidx uint32, didx uint32, VisitData float64)
type ConvergeFunc[VertexProp, EdgeProp any] func(g *Graph[VertexProp, EdgeProp], wg *sync.WaitGroup)
type EdgeParserFunc[EdgeProp any] func(lineText string) RawEdge[EdgeProp]

// Note: for now we have fake edge weights where the weight is just 1.
// This can be adjusted in the future by just adjusting the constructor
// and retrieval process here. But the edge weight consumes a lot of memory
// for large graphs and should ideally be optional. A better way to
// do this dynamically would be nice.
// Will also need to think about a better way to have templated edge types
// if we are exploring edges with timestamps.
type Edge[EdgeProp any] struct {
	Target uint32
	Property EdgeProp
}

func NewEdge[EdgeProp any](target uint32, property *EdgeProp) Edge[EdgeProp] {
	return Edge[EdgeProp]{target, *property}
}

func (*Edge[EdgeProp]) GetWeight() float64 {
	return 1.0
}

// Edge t
type InEdge struct {
	Target uint32
	Weight float64
}

func (e *Edge[EdgeProp]) Reset() {
	*e = Edge[EdgeProp]{}
}

func (e *InEdge) Reset() {
	*e = InEdge{}
}

// Vertex Main type defining a vertex in a graph. Contains necessary per-vertex information for structure and identification.
type Vertex[VertexProp, EdgeProp any] struct {
	Id       uint32           // Raw (external) ID of a vertex, reflecting the external original identifier of a vertex, NOT the internal [0, N] index.
	Scratch  float64          // Common scratchpad / accumulator, also used to track activity.
	OutEdges []Edge[EdgeProp] // Main outgoing edgelist.
	InEdges  []InEdge         // Incoming edges (currently unused).
	Mutex    sync.Mutex       // Mutex for thread synchroniziation, if needed.
	Property VertexProp       // Generic property type, can be variable per algorithm.
}

func (v *Vertex[VertexProp, EdgeProp]) Reset() {
	*v = Vertex[VertexProp, EdgeProp]{}
	for eidx := range v.OutEdges {
		v.OutEdges[eidx].Reset()
	}
	for uidx := range v.InEdges {
		v.InEdges[uidx].Reset()
	}
}

func (v *Vertex[VertexProp, EdgeProp]) ToThreadIdx() uint32 {
	return v.Id % uint32(THREADS)
}

func (g *Graph[VertexProp, EdgeProp]) RawIdToThreadIdx(RawId uint32) uint32 {
	return RawId % uint32(THREADS)
}

func (g *Graph[VertexProp, EdgeProp]) Reset() {
	for vidx := range g.Vertices {
		g.Vertices[vidx].Reset()
	}
}

// ComputeInEdges update all vertices' InEdges to match the edges stored in OutEdges lists
func (g *Graph[VertexProp, EdgeProp]) ComputeInEdges() {
	for vidx := range g.Vertices {
		for eidx := range g.Vertices[vidx].OutEdges {
			target := int(g.Vertices[vidx].OutEdges[eidx].Target)
			g.Vertices[target].InEdges = append(g.Vertices[target].InEdges, InEdge{uint32(vidx), 0.0})
		}
	}
	info("Computed inbound edges.")
}

// ComputeGraphStats prints some statistics of the graph
func (g *Graph[VertexProp, EdgeProp]) ComputeGraphStats(inDeg bool, outDeg bool) {
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

func (g *Graph[VertexProp, EdgeProp]) PrintStructure() {
	log.Println(g.VertexMap)
	for vidx := range g.Vertices {
		pr := fmt.Sprintf("%d", g.Vertices[vidx].Id)
		el := ""
		for _, e := range g.Vertices[vidx].OutEdges {
			el += fmt.Sprintf("%d, ", g.Vertices[e.Target].Id)
		}
		log.Println(pr + ": " + el)
	}
}

func (g *Graph[VertexProp, EdgeProp]) PrintVertexInEdgeSum(prefix string) {
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
