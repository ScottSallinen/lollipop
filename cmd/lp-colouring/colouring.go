package main

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
)

const EmptyColour = math.MaxUint32

type VertexProperty struct {
	NbrColours sync.Map
	WaitCount  int64
	Colour     uint32
}

type EdgeProperty struct{}

func (p *VertexProperty) String() string {
	s := fmt.Sprintf("{%d,%d,[", p.Colour, p.WaitCount)
	p.NbrColours.Range(func(key, value any) bool {
		s += fmt.Sprintf("%d:%d,", key, value)
		return true
	})
	return s + "]}"
}

func hash(id uint32) (hash uint32) {
	// TODO: dummy hash function
	return id
}

func comparePriority(p1, p2 uint32, id1, id2 uint32) bool {
	return p1 > p2 || (p1 == p2 && id1 > id2)
}

// findFirstUnused finds the smallest unused colour, modifies colours, O(n)
func findFirstUnused(colours []uint32) (firstUnused uint32) {
	// There must be an available colour <= length
	// similar to https://leetcode.com/problems/first-missing-positive/
	length := uint32(len(colours))
	for i := range colours {
		for colours[i] < length && colours[i] != colours[colours[i]] {
			colours[i], colours[colours[i]] = colours[colours[i]], colours[i]
		}
	}
	for i, colour := range colours {
		if uint32(i) != colour {
			return uint32(i)
		}
	}
	return length
}

func MessageAggregator(target, source *graph.Vertex[VertexProperty, EdgeProperty], data float64) (newInfo bool) {
	colour := uint32(data)
	// info(fmt.Sprintf("MessageAggregator target.Id=%v source.Id=%v data=%v colour=%v", target.Id, source.Id, data, colour))
	target.Property.NbrColours.Store(source.Id, colour)

	if atomic.LoadInt64(&target.Property.WaitCount) > 0 {
		newWaitCount := atomic.AddInt64(&target.Property.WaitCount, -1)
		return newWaitCount <= 0 // newWaitCount might go below 0
	}

	// If we have priority, there is no need to update check our colour
	if comparePriority(hash(target.Id), hash(source.Id), target.Id, source.Id) {
		return false
	}

	return true
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) float64 {
	return 0
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty], vidx uint32) {
	v := &g.Vertices[vidx]

	v.Scratch = g.EmptyVal // Set this to empty to prevent sync.go from always visiting the vertex
	v.Property.Colour = EmptyColour

	// Initialize WaitCount
	myPriority := hash(v.Id)
	waitCount := int64(0)
	for i := range v.OutEdges {
		edge := &v.OutEdges[i]
		targetId := g.Vertices[edge.Destination].Id // we need to access the target ID to get the priority
		edgePriority := hash(targetId)
		if comparePriority(edgePriority, myPriority, targetId, v.Id) {
			// no need to lock
			waitCount++
		}
	}
	atomic.StoreInt64(&v.Property.WaitCount, waitCount)
}

func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty], sidx uint32, didxStart int, data float64) {
	source := &g.Vertices[sidx]
	sourcePriority := hash(source.Id)

	for dstIndex := didxStart; dstIndex < len(source.OutEdges); dstIndex++ {
		dstId := g.Vertices[dstIndex].Id // we need to access the target ID to get the priority
		targetPriority := hash(dstId)
		// If we have priority, tell the other vertex to check their colour
		if comparePriority(sourcePriority, targetPriority, source.Id, dstId) {
			g.OnQueueVisit(g, sidx, uint32(dstIndex), float64(source.Property.Colour))
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty], sidx uint32, didx uint32, data float64) {
	source := &g.Vertices[sidx]
	destinationId := g.Vertices[didx].Id

	destinationColour, ok := source.Property.NbrColours.Load(destinationId)
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
		g.OnQueueVisit(g, sidx, source.OutEdges[i].Destination, float64(newColour))
	}
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty], vidx uint32, data float64) int {
	v := &g.Vertices[vidx]

	if atomic.LoadInt64(&v.Property.WaitCount) > 0 {
		return 0
	}

	// len(v.OutEdges) is only an approximated size, as the reverse edge might not have been added yet
	colours := make([]uint32, 0, len(v.OutEdges))
	v.Property.NbrColours.Range(func(key any, value any) bool {
		colour := value.(uint32)
		colours = append(colours, colour)
		return true
	})

	var newColour uint32
	if v.Property.Colour == EmptyColour {
		newColour = findFirstUnused(colours)
	} else {
		// Dynamic graph
		// There are more efficient ways to do this, if we have more information about the new neighbour
		newColour = findFirstUnused(colours)
		if newColour == v.Property.Colour {
			return 0
		}
	}
	v.Property.Colour = newColour
	for i := range v.OutEdges {
		g.OnQueueVisit(g, vidx, v.OutEdges[i].Destination, float64(newColour))
	}
	return len(v.OutEdges)
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty]) error {
	return nil
}
