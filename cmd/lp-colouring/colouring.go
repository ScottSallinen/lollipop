package main

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/kelindar/bitmap"
)

const EMPTYVAL = math.MaxUint32

type VertexProperty struct {
	NbrColours sync.Map
	WaitCount  int64
	Colour     uint32
}

func (p *VertexProperty) String() string {
	s := fmt.Sprintf("{%d,%d,[", p.Colour, p.WaitCount)
	p.NbrColours.Range(func(key, value any) bool {
		s += fmt.Sprintf("%d:%d,", key, value)
		return true
	})
	return s + "]}"
}

type EdgeProperty struct{}

type MessageValue uint32

func hash(id uint32) (hash uint32) {
	// TODO: dummy hash function
	return id
}

func comparePriority(p1, p2 uint32, id1, id2 uint32) bool {
	return p1 > p2 || (p1 == p2 && id1 > id2)
}

// findFirstUnused finds the smallest unused index.
func findFirstUnused(coloursIndexed bitmap.Bitmap) (firstUnused uint32) {
	firstUnused, _ = coloursIndexed.MinZero()
	return firstUnused
}

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, data MessageValue) (newInfo bool) {
	colour := uint32(data)
	if didx != sidx { // Self edges shouldn't refuse us our own colour
		dst.Property.NbrColours.Store(sidx, colour)
	}

	if atomic.LoadInt64(&dst.Property.WaitCount) > 0 {
		newWaitCount := atomic.AddInt64(&dst.Property.WaitCount, -1)
		return newWaitCount <= 0 // newWaitCount might go below 0
	}

	// If we have priority, there is no need to update check our colour
	if comparePriority(hash(didx), hash(sidx), didx, sidx) {
		return false
	}

	return true
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) MessageValue {
	return 0
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) {
	v := &g.Vertices[vidx]

	v.Property.Colour = EMPTYVAL

	// Initialize WaitCount
	myPriority := hash(vidx)
	waitCount := int64(0)
	for i := range v.OutEdges {
		edge := &v.OutEdges[i]
		edgePriority := hash(edge.Destination)
		if comparePriority(edgePriority, myPriority, edge.Destination, vidx) {
			// no need to lock
			waitCount++
		}
	}
	atomic.StoreInt64(&v.Property.WaitCount, waitCount)
}

// OnEdgeAdd: Function called upon a new edge add (which also bundes a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: didxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
// For colouring, data here is unused.
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, data MessageValue) {
	source := &g.Vertices[sidx]
	sourcePriority := hash(sidx)

	for eidx := didxStart; eidx < len(source.OutEdges); eidx++ {
		dstIndex := source.OutEdges[eidx].Destination
		dstPriority := hash(dstIndex)
		// If we have priority, tell the other vertex to check their colour
		if comparePriority(sourcePriority, dstPriority, sidx, dstIndex) {
			g.OnQueueVisit(g, sidx, dstIndex, MessageValue(source.Property.Colour))
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didx uint32, data MessageValue) {
	source := &g.Vertices[sidx]

	destinationColour, ok := source.Property.NbrColours.Load(didx)
	if !ok {
		return
	}

	newColour := destinationColour.(uint32)
	if newColour >= source.Property.Colour {
		// Only want to take a smaller colour
		return
	}

	conflict := false
	source.Property.NbrColours.Range(func(key, value any) bool {
		if value.(uint32) == newColour {
			conflict = true
			return false
		}
		return true
	})
	if conflict {
		return
	}

	source.Property.Colour = newColour
	for i := range source.OutEdges {
		g.OnQueueVisit(g, sidx, source.OutEdges[i].Destination, MessageValue(newColour))
	}
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, data MessageValue) int {
	v := &g.Vertices[vidx]

	if atomic.LoadInt64(&v.Property.WaitCount) > 0 {
		return 0
	}

	// It's okay if there's newer data in the map here -- we'll let the bitmap grow.
	var coloursIndexed bitmap.Bitmap
	v.Property.NbrColours.Range(func(key any, value any) bool {
		coloursIndexed.Set(value.(uint32)) // this bitmap grows as needed
		return true
	})

	var newColour uint32
	if v.Property.Colour == EMPTYVAL {
		newColour = findFirstUnused(coloursIndexed)
	} else {
		// Dynamic graph
		// There are more efficient ways to do this, if we have more information about the new neighbour
		newColour = findFirstUnused(coloursIndexed)
		if newColour == v.Property.Colour {
			return 0
		}
	}
	v.Property.Colour = newColour
	for i := range v.OutEdges {
		g.OnQueueVisit(g, vidx, v.OutEdges[i].Destination, MessageValue(newColour))
	}
	return len(v.OutEdges)
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
