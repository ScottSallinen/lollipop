package main

import (
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

const EMPTYVAL = math.MaxUint32

type VertexProperty struct {
	Value   uint32
	Scratch uint32 // Intermediary accumulator
}

func (p *VertexProperty) String() string {
	return fmt.Sprintf("%.4f", p.Value)
}

type EdgeProperty struct {
	Weight float64
}

type MessageValue uint32

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, data MessageValue) (newInfo bool) {
	dst.Mutex.Lock()
	tmp := dst.Property.Scratch
	// Labels decrease monotonically
	if uint32(data) < dst.Property.Scratch {
		dst.Property.Scratch = uint32(data)
	}
	newInfo = tmp != dst.Property.Scratch
	dst.Mutex.Unlock()
	return newInfo
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) MessageValue {
	// We can leave Scratch alone, since we are monotonicly decreasing.
	target.Mutex.Lock()
	tmp := target.Property.Scratch
	target.Mutex.Unlock()
	return MessageValue(tmp)
}


// set vidx as label
func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) {
	// set id as label, type is float64, change to other later
	g.Vertices[vidx].Property.Value = uint32(g.Vertices[vidx].Id)
	//g.Vertices[vidx].Property.Value = float64(vidx)
	g.Vertices[vidx].Property.Scratch = uint32(g.Vertices[vidx].Id)
	//g.Vertices[vidx].Property.Scratch = float64(vidx)
}

// OnEdgeAdd: Function called upon a new edge add (which also bundes a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: didxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, data MessageValue) {
	if OnVisitVertex(g, sidx, data) > 0 {
		// do nothing, we had messaged all edges
	} else {
		src := &g.Vertices[sidx]
		if src.Property.Value < EMPTYVAL { // Only useful if we are connected
			// Message only new edges.
			for eidx := didxStart; eidx < len(src.OutEdges); eidx++ {
				target := src.OutEdges[eidx].Destination
				g.OnQueueVisit(g, sidx, target, MessageValue(src.Property.Value))
			}
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didx uint32, data MessageValue) {
	enforce.ENFORCE(false, "Incremental only algorithm")
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, data MessageValue) int {
	src := &g.Vertices[vidx]
	// Only act on an improvement to shortest path.
	if src.Property.Value > uint32(data) {
		// Update our own value.
		src.Property.Value = uint32(data)
		// Send an update to all neighbours.
		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Destination
			g.OnQueueVisit(g, vidx, target, MessageValue(src.Property.Value))
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
