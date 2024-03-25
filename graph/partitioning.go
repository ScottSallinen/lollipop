package graph

import (
	"math/rand"

	"github.com/ScottSallinen/lollipop/utils"
)

var numVertices uint32 = 0

const PARTITIONING_BATCH_SIZE = 1024

// Bulk placement

func (g *Graph[V, E, M, N]) FindVertexPlacementBulk(eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], numEvents int, undirected bool) {
	g.FindVertexPlacementBulkIndividual(eventBatch, eventBatchPlacement, numEvents, undirected)
}

func (g *Graph[V, E, M, N]) FindVertexPlacementBulkIndividual(eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], numEvents int, undirected bool) {
	for i := 0; i < numEvents; i++ {
		eventBatchPlacement[i].First, eventBatchPlacement[i].Second = g.FindVertexPlacement(eventBatch[i], undirected)
	}
}

// Individual placement

func (g *Graph[V, E, M, N]) FindVertexPlacement(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	return g.FindVertexPlacementModulo(edgeEvent, undirected)
}

func (g *Graph[V, E, M, N]) FindVertexPlacementModulo(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		tidx := edgeEvent.SrcRaw.Integer() % g.NumThreads
		srcId = g.addMapping(tidx, edgeEvent.SrcRaw)
	}
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		tidx := edgeEvent.DstRaw.Integer() % g.NumThreads
		dstId = g.addMapping(tidx, edgeEvent.DstRaw)
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementSkewed(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		srcId = g.addMapping(0, edgeEvent.SrcRaw)
	}
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		dstId = g.addMapping(0, edgeEvent.DstRaw)
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementRandom(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		srcId = g.addMapping(rand.Uint32()%g.NumThreads, edgeEvent.SrcRaw)
	}
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		dstId = g.addMapping(rand.Uint32()%g.NumThreads, edgeEvent.DstRaw)
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementRoundRobin(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		srcId = g.addMapping(numVertices%g.NumThreads, edgeEvent.SrcRaw)
	}
	numVertices += 1
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		dstId = g.addMapping(numVertices%g.NumThreads, edgeEvent.DstRaw)
	}
	numVertices += 1
	return
}

func (g *Graph[V, E, M, N]) addMapping(tidx uint32, rawId RawType) (internalId uint32) {
	internalId = InternalCompress(tidx, g.ThreadVertexCounts[tidx])
	g.ThreadVertexCounts[tidx] += 1
	g.VertexMap[rawId] = internalId
	return internalId
}
