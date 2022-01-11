package graph

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
)

func info(args ...interface{}) {
	log.Println("[Graph]\t", fmt.Sprint(args...))
}

// Graph t
type Graph struct {
	GraphLock    sync.RWMutex
	VertexMap    map[uint32]uint32
	Vertices     []Vertex `json:"vertices"`
	OnQueueVisit OnQueueVisitFunc
	MessageQ     []chan Message
	MsgCounters  []int
}

type Message struct {
	Src uint32
	Dst uint32
	Val float64
}

type OnQueueVisitFunc func(g *Graph, Src uint32, Dst uint32, VisitData interface{})

// Edge t
type Edge struct {
	Target uint32 `json:"target"`
	//Prop   float64 `json:"prop"`
	//Weight float64 `json:"weight"`
}

// Edge t
type InEdge struct {
	Target uint32  `json:"target"`
	Prop   float64 `json:"prop"`
	//Weight float64 `json:"weight"`
}

func (e *Edge) Reset() {
	//e.Prop = 0.0
}

func (e *InEdge) Reset() {
	e.Prop = 0.0
}

// Vertex t
type Vertex struct {
	Id uint32 `json:"id"`
	//Weight      float64    `json:"weight"`
	Properties VertexProp `json:"properties"`
	OutEdges   []Edge     `json:"outedges"`
	InEdges    []InEdge   `json:"inedges"`
	Scratch    float64
	Active     bool
	Mu         sync.Mutex
}

type VertexProp struct {
	Value    float64
	Residual float64
	Latent   float64
}

func (v *Vertex) Reset() {
	v.Properties = VertexProp{}
	v.Scratch = 0
	for eidx := range v.OutEdges {
		v.OutEdges[eidx].Reset()
	}
	for uidx := range v.InEdges {
		v.InEdges[uidx].Reset()
	}
}

func (g *Graph) Reset() {
	for vidx := range g.Vertices {
		g.Vertices[vidx].Reset()
	}
}

// ComputeInEdges t
func (g *Graph) ComputeInEdges() {
	for vidx := range g.Vertices {
		for eidx := range g.Vertices[vidx].OutEdges {
			target := int(g.Vertices[vidx].OutEdges[eidx].Target)
			g.Vertices[target].InEdges = append(g.Vertices[target].InEdges, InEdge{uint32(vidx), 0.0})
		}
	}
	info("Computed inbound edges.")
}

func (graph *Graph) Densify() {
	for vidx := range graph.Vertices {
		if len(graph.Vertices[vidx].OutEdges) == 0 {
			for tidx := range graph.Vertices {
				if tidx != vidx {
					graph.Vertices[vidx].OutEdges = append(graph.Vertices[vidx].OutEdges, Edge{uint32(tidx)})
				}
			}
		}
	}
	info("Densification complete.")
}

func MaxUint64(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}
func MaxFloat64(x, y float64) float64 {
	if x < y {
		return y
	}
	return x
}
func MinFloat64(x, y float64) float64 {
	if y < x {
		return y
	}
	return x
}
func Median(n []int) int {
	sort.Ints(n) // sort numbers
	idx := len(n) / 2
	if len(n)%2 == 0 { // even
		return n[idx]
	}
	return (n[idx-1] + n[idx]) / 2
}
func MedianFloat64(n []float64) float64 {
	sort.Float64s(n) // sort numbers
	idx := len(n) / 2
	if len(n)%2 == 0 { // even
		return n[idx]
	}
	return (n[idx-1] + n[idx]) / 2
}

func (g *Graph) ComputeGraphStats(inDeg bool, outDeg bool) {
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
			maxOutDegree = MaxUint64(uint64(len(g.Vertices[vidx].OutEdges)), maxOutDegree)
			listOutDegree = append(listOutDegree, len(g.Vertices[vidx].OutEdges))
		}
		if inDeg {
			maxInDegree = MaxUint64(uint64(len(g.Vertices[vidx].InEdges)), maxInDegree)
			listInDegree = append(listInDegree, len(g.Vertices[vidx].InEdges))
		}
	}

	info("---- Graph Stats ----")
	info("Vertices : ", len(g.Vertices))
	info("Sinks : ", numSinks, " pct:", fmt.Sprintf("%.3f", float64(numSinks)*100.0/float64(len(g.Vertices))))
	info("Edges : ", numEdges)
	if outDeg {
		info("MaxOutDeg : ", maxOutDegree)
		info("MedianOutDeg : ", Median(listOutDegree))
	}
	if inDeg {
		info("MaxInDeg : ", maxInDegree)
		info("MedianInDeg : ", Median(listInDegree))
	}
	info("---- End of Stats ----")
}

func ResultCompare(a []float64, b []float64) {
	largestDiff := float64(0)
	smallestDiff := float64(0)
	avgDiff := float64(0)
	listDiff := []float64{}

	for idx := range a {
		delta := math.Abs((b[idx] - a[idx]) * 100.0 / a[idx])
		listDiff = append(listDiff, delta)
		avgDiff += delta
		largestDiff = MaxFloat64(largestDiff, delta)
		smallestDiff = MinFloat64(smallestDiff, delta)
	}
	avgDiff = avgDiff / float64(len(a))

	medianDiff := MedianFloat64(listDiff)

	info("---- Result Compare ----")
	info("largestDiff : ", largestDiff)
	info("smallestDiff : ", smallestDiff)
	info("avgDiff : ", avgDiff)
	info("medianDiff : ", medianDiff)
	info("---- End of Compare ----")
}

func (g *Graph) PrintVertexProps(prefix string) {
	top := prefix
	sum := 0.0
	for vidx := range g.Vertices {
		top += fmt.Sprintf("%.3f", g.Vertices[vidx].Properties.Value) + " "
		sum += g.Vertices[vidx].Properties.Value
	}
	info(top + " : " + fmt.Sprintf("%.3f", sum))
}

func (g *Graph) GetVertexProps() []float64 {
	props := []float64{}
	for vidx := range g.Vertices {
		props = append(props, g.Vertices[vidx].Properties.Value)
	}
	return props
}

func (g *Graph) PrintVertexPropsDiv(prefix string, factor float64) {
	top := prefix
	sum := 0.0
	for vidx := range g.Vertices {
		top += fmt.Sprintf("%.3f", g.Vertices[vidx].Properties.Value/factor) + " "
		sum += g.Vertices[vidx].Properties.Value / factor
	}
	info(top + " : " + fmt.Sprintf("%.3f", sum))
}

func (g *Graph) PrintVertexPropsNorm(prefix string, norm float64) {
	top := prefix
	sum := 0.0
	for vidx := range g.Vertices {
		sum += g.Vertices[vidx].Properties.Value
	}
	normMult := norm / sum
	for vidx := range g.Vertices {
		top += fmt.Sprintf("%.3f", g.Vertices[vidx].Properties.Value*normMult) + " "
	}
	info(top + " : " + fmt.Sprintf("%.3f", norm) + " (from " + fmt.Sprintf("%.3f", sum) + ")")
}

func (g *Graph) PrintVertexInEdgeSum(prefix string) {
	top := prefix
	sum := 0.0
	for vidx := range g.Vertices {
		localsum := 0.0
		for eidx := range g.Vertices[vidx].InEdges {
			localsum += g.Vertices[vidx].InEdges[eidx].Prop
		}
		sum += localsum
		top += fmt.Sprintf("%.3f", localsum) + " "
	}
	info(top + " : " + fmt.Sprintf("%.3f", sum))
}
