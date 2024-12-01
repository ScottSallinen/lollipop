package main

import (
	"fmt"
	"math"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

const EMPTYVAL = math.MaxFloat64

type PathProperty struct {
	Weight float64
	Timestamp float64
}

// Sort by timestamp from small to large
type PathSet []PathProperty

type VertexProperty struct {
	Value   PathSet
	Scratch PathSet // Intermediary accumulator
}

type MessageValue PathSet

type EdgeProperty struct {
	Weight float64
	Timestamp float64
}

func (p *VertexProperty) String() string {
	var str string
	for _, path := range p.Value {
		str += fmt.Sprintf("{weight: %.4f timestamp: %.4f} ", path.Weight, path.Timestamp)
	}
	return "{" + str + "}"
}

// Check if paths forwardable at timestamp
func IsAbleForward(paths PathSet, timestamp float64) bool {
	return len(paths) > 0 && paths[0].Timestamp <= timestamp
}

// Forward paths through edge and get one path
func ForwardPathSet(paths PathSet, edge EdgeProperty) PathProperty {
	minWeight := EMPTYVAL
	for _, path := range paths {
		if path.Timestamp <= edge.Timestamp && path.Weight < minWeight {
			minWeight = path.Weight
		}
	}
	return PathProperty{Weight: minWeight + edge.Weight, Timestamp: edge.Timestamp}
}

// Check if message can update paths
func IsAbleUpdate(paths PathSet, message PathProperty) bool {
	if (len(paths) == 0 || paths[0].Timestamp > message.Timestamp || (paths[0].Timestamp == message.Timestamp && paths[0].Weight > message.Weight)) {
		return true;
	}
	n := len(paths)
	for k := 1; k < n; k++ {
		if (paths[k-1].Timestamp < message.Timestamp && message.Timestamp < paths[k].Timestamp && message.Weight < paths[k-1].Weight) || 
			(message.Timestamp == paths[k].Timestamp && message.Weight < paths[k].Weight) {
			return true
		}
	}
	return false
}

func UpdatePathSet(paths PathSet, messages MessageValue) (PathSet, bool) {
	newInfo := false
	if len(paths) == 0 {
		return PathSet(messages), true
	}
	for _, message := range messages {
		p := 0
		n := len(paths)
		for ; p < n && paths[p].Timestamp < message.Timestamp; p++ {}
		// add to tail
		if p == n {
			if paths[p-1].Weight > message.Weight {
				paths = append(paths, message)
				newInfo = true
			}
			continue
		}
		if (paths[p].Timestamp == message.Timestamp && paths[p].Weight <= message.Weight) ||
			(p > 0 && paths[p-1].Weight <= message.Weight) {
			continue
		}
		// add before tail
		q := p
		for ; q < n && paths[q].Weight >= message.Weight; q++ {}
		newpaths := PathSet{}
		if p > 0 {
			newpaths = paths[:p-1]
		}
		newpaths = append(newpaths, message)
		if q < n {
			newpaths = append(newpaths, paths[q:n]...)
		}
		paths = newpaths
		newInfo = true
	}
	return paths, newInfo
}

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, messages MessageValue) (newInfo bool) {
	dst.Mutex.Lock()
	dst.Property.Scratch, newInfo = UpdatePathSet(dst.Property.Scratch, messages)
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
	g.Vertices[vidx].Property.Value = PathSet{}
	g.Vertices[vidx].Property.Scratch = PathSet{}
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
					g.OnQueueVisit(g, sidx, target, MessageValue{ForwardPathSet(src.Property.Value, src.OutEdges[eidx].Property)})
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
	isUpdate := false
	src.Property.Value, isUpdate = UpdatePathSet(src.Property.Value, data)
	if isUpdate {
		// Send an update to all neighbours.
		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Destination
			// The edges' timestamps on the path are not descending.
			if IsAbleForward(src.Property.Value, src.OutEdges[eidx].Property.Timestamp) {
				g.OnQueueVisit(g, vidx, target, MessageValue{ForwardPathSet(src.Property.Value, src.OutEdges[eidx].Property)})
			}
		}
		return len(src.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
