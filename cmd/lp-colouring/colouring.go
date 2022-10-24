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
	NbrColoursScratch sync.Map
	NbrColours        map[uint32]uint32
	WaitCount         int64
	Colour            uint32
}

func (p *VertexProperty) String() string {
	s := fmt.Sprintf("{%d,%d,[", p.Colour, p.WaitCount)
	for k, v := range p.NbrColours {
		s += fmt.Sprintf("%d:%d,", k, v)
	}
	return s + "]}"
}

type EdgeProperty struct{}

type IdColourPair struct {
	Vidx   uint32
	Colour uint32
}

type MessageValue []IdColourPair

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

func MessageAggregator(dst *graph.Vertex[VertexProperty, EdgeProperty], didx, sidx uint32, VisitMsg MessageValue) (newInfo bool) {
	// Self edges shouldn't refuse us our own colour & nil message is init message.
	if didx != sidx && VisitMsg != nil {
		colour := uint32(VisitMsg[0].Colour) // Queue visits should only send a single value for this algorithm
		dst.Mutex.RLock()                    // Share write access
		dst.Property.NbrColoursScratch.Store(sidx, colour)
		dst.Mutex.RUnlock()
	}

	if atomic.LoadInt64(&dst.Property.WaitCount) > 0 {
		newWaitCount := atomic.AddInt64(&dst.Property.WaitCount, -1)
		return newWaitCount <= 0 // newWaitCount might go below 0
	}

	// If we have priority, there is no need to update check our colour
	if comparePriority(hash(didx), hash(sidx), didx, sidx) {
		return false
	}

	// Ensure we only notify the target once.
	active := (atomic.SwapInt32(&dst.IsActive, 1) == 0)
	return active
}

func AggregateRetrieve(target *graph.Vertex[VertexProperty, EdgeProperty]) MessageValue {
	atomic.StoreInt32(&target.IsActive, 0)
	ret := []IdColourPair{}
	target.Mutex.Lock() // Full lock, un-shared read
	target.Property.NbrColoursScratch.Range(func(key, value any) bool {
		ret = append(ret, IdColourPair{Vidx: key.(uint32), Colour: value.(uint32)})
		return true
	})
	target.Property.NbrColoursScratch = sync.Map{}
	target.Mutex.Unlock()
	return ret
}

func OnInitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) {
	v := &g.Vertices[vidx]

	v.Property.Colour = EMPTYVAL
	v.Property.NbrColours = make(map[uint32]uint32)

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
func OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, didxStart int, VisitMsg MessageValue) {
	source := &g.Vertices[sidx]
	sourcePriority := hash(sidx)

	for _, v := range VisitMsg {
		source.Property.NbrColours[v.Vidx] = v.Colour
	}

	for eidx := didxStart; eidx < len(source.OutEdges); eidx++ {
		dstIndex := source.OutEdges[eidx].Destination
		dstPriority := hash(dstIndex)
		// If we have priority, tell the other vertex to check their colour
		if comparePriority(sourcePriority, dstPriority, sidx, dstIndex) {
			g.OnQueueVisit(g, sidx, dstIndex, []IdColourPair{{Vidx: sidx, Colour: source.Property.Colour}})
		}
	}
}

func OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], sidx uint32, deletedEdges []graph.Edge[EdgeProperty], VisitMsg MessageValue) {
	source := &g.Vertices[sidx]

	// Could be optimized by merging
	OnVisitVertex(g, sidx, VisitMsg)

	myNewColour := source.Property.Colour
	for _, e := range deletedEdges {
		potentialNewColour, ok := source.Property.NbrColours[e.Destination]
		if !ok {
			continue
		}

		if potentialNewColour >= myNewColour {
			// Only want to take a smaller colour
			continue
		}

		for _, v := range source.Property.NbrColours {
			if v == potentialNewColour {
				continue
			}
		}
		myNewColour = potentialNewColour
	}

	if myNewColour != source.Property.Colour {
		source.Property.Colour = myNewColour
		for i := range source.OutEdges {
			g.OnQueueVisit(g, sidx, source.OutEdges[i].Destination, []IdColourPair{{Vidx: sidx, Colour: myNewColour}})
		}
	}
}

func OnVisitVertex(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, VisitMsg MessageValue) int {
	vertex := &g.Vertices[vidx]

	for _, v := range VisitMsg {
		vertex.Property.NbrColours[v.Vidx] = v.Colour
	}

	if atomic.LoadInt64(&vertex.Property.WaitCount) > 0 {
		return 0
	}

	var coloursIndexed bitmap.Bitmap
	size := uint32(len(vertex.Property.NbrColours))
	coloursIndexed.Grow(size)
	for _, v := range vertex.Property.NbrColours {
		if v <= size {
			coloursIndexed.Set(v)
		}
	}

	var newColour uint32
	if vertex.Property.Colour == EMPTYVAL {
		newColour = findFirstUnused(coloursIndexed)
	} else {
		// Dynamic graph
		// There are more efficient ways to do this, if we have more information about the new neighbour
		newColour = findFirstUnused(coloursIndexed)
		if newColour == vertex.Property.Colour {
			return 0
		}
	}
	vertex.Property.Colour = newColour
	for i := range vertex.OutEdges {
		g.OnQueueVisit(g, vidx, vertex.OutEdges[i].Destination, []IdColourPair{{Vidx: vidx, Colour: newColour}})
	}
	return len(vertex.OutEdges)
}

func OnFinish(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue]) error {
	return nil
}
