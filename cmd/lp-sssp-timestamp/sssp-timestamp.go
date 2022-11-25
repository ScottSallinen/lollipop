package main

import (
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

const EMPTYVAL = math.MaxFloat64

// use struct{} instead of bool to save memory
type VoidItem struct{}

var void_member VoidItem

type PathProperty struct {
	Weight float64
	Timestamp float64
}

type PathMap map[PathProperty]VoidItem

type VertexProperty struct {
	Value   PathMap
	Scratch PathMap // Intermediary accumulator
}

type MessageValue PathMap

type EdgeProperty struct {
	Weight float64
	Timestamp float64
}

func (p *VertexProperty) String() string {
	var str string
	for path := range p.Value {
		str += fmt.Sprintf("{weight: %.4f timestamp: %.4f} ", path.Weight, path.Timestamp)
	}
	return "{" + str + "}"
}

func IsAbleForward(set PathMap, timestamp float64) bool {
	for path := range set {
		if path.Timestamp <= timestamp {
			return true
		}
	}
	return false
}

func ForwardPathSet(set PathMap, edge EdgeProperty) PathMap {
	newset := make(PathMap)
	for path := range set {
		if path.Timestamp <= edge.Timestamp {
			newset[PathProperty{Weight: path.Weight + edge.Weight, Timestamp: edge.Timestamp}] = void_member
		}
	}
	return newset
}

func IsAlbeUpdate(set PathMap, data PathMap) bool {
	if len(data) == 0 {
		return false
	}
	for message := range data {
		_, exists := set[message]
		if (exists) {
			continue
		}
		if (len(set) == 0) {
			return true;
		}
		for path := range set {
			// remove from set
			if (message.Weight <= path.Weight) && (message.Timestamp <= path.Timestamp) && (!(message.Weight == path.Weight) && (message.Timestamp == path.Timestamp)) {
				return true
			}
			// add_message
			if (message.Weight < path.Weight) || (message.Timestamp < path.Timestamp) {
				return true
			}
		}
	}
	return false
}

func UpdatePathSet(set PathMap, data MessageValue) (newInfo bool) {
	newInfo = false
	for message := range data {
		_, exists := set[message]
		if (exists) {
			continue
		}
		add_message := len(set) == 0
		for path := range set {
			if (message.Weight <= path.Weight) && (message.Timestamp <= path.Timestamp) && (!(message.Weight == path.Weight) && (message.Timestamp == path.Timestamp)) {
				delete(set, path)
				newInfo = true
			}
			if !add_message && ((message.Weight < path.Weight) || (message.Timestamp < path.Timestamp)) {
				add_message = true
			}
		}
		if add_message {
			set[message] = void_member
			newInfo = true
		}
	}
	return newInfo
}

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, data MessageValue) (newInfo bool) {
	dst.Mutex.Lock()
	newInfo = UpdatePathSet(dst.Property.Scratch, data)
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

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) {
	g.Vertices[vidx].Property.Value = make(PathMap)
	g.Vertices[vidx].Property.Scratch = make(PathMap)
}

// OnEdgeAdd: Function called upon a new edge add (which also bundes a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: didxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, data MessageValue) {
	if OnVisitVertex(g, sidx, data) > 0 {
		// do nothing, we had messaged all edges
	} else {
		src := &g.Vertices[sidx]
		if len(src.Property.Value) > 0 { // Only useful if we are connected
			// Message only new edges.
			for eidx := didxStart; eidx < len(src.OutEdges); eidx++ {
				target := src.OutEdges[eidx].Destination
				// The edges' timestamps on the path are not descending.
				if IsAbleForward(src.Property.Value, src.OutEdges[eidx].Property.Timestamp) {
					g.OnQueueVisit(g, sidx, target, MessageValue(ForwardPathSet(src.Property.Value, src.OutEdges[eidx].Property)))
				}
			}
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, deletedEdges []graph.Edge[EdgeProperty], data MessageValue) {
	enforce.ENFORCE(false, "Incremental only algorithm")
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, data MessageValue) int {
	src := &g.Vertices[vidx]
	// Only act on an improvement to shortest path.
	// Update our own value.
	isUpdate := UpdatePathSet(src.Property.Value, data)
	if isUpdate {
		// Send an update to all neighbours.
		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Destination
			// The edges' timestamps on the path are not descending.
			if IsAbleForward(src.Property.Value, src.OutEdges[eidx].Property.Timestamp) {
				g.OnQueueVisit(g, vidx, target, MessageValue(ForwardPathSet(src.Property.Value, src.OutEdges[eidx].Property)))
			}
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
