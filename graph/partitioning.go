package graph

import (
	"math/rand"
)

var numVertices uint32 = 0

func (g *Graph[V, E, M, N]) FindVertexPlacement(edgeEvent TopologyEvent[E], undirected bool, fails *int) (srcId uint32, dstId uint32) {
	return g.FindVertexPlacementModulo(edgeEvent, undirected, fails)
}

func (g *Graph[V, E, M, N]) FindVertexPlacementModulo(edgeEvent TopologyEvent[E], undirected bool, fails *int) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		tidx := edgeEvent.SrcRaw.Integer() % g.NumThreads // TODO: Use a better placement
		srcId = g.addMapping(tidx, edgeEvent.SrcRaw)
	}
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		tidx := edgeEvent.DstRaw.Integer() % g.NumThreads // TODO: Use a better placement
		dstId = g.addMapping(tidx, edgeEvent.DstRaw)
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementSkewed(edgeEvent TopologyEvent[E], undirected bool, fails *int) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		srcId = g.addMapping(0, edgeEvent.SrcRaw)
	}
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		dstId = g.addMapping(0, edgeEvent.DstRaw)
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementRandom(edgeEvent TopologyEvent[E], undirected bool, fails *int) (srcId uint32, dstId uint32) {
	var ok bool
	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		srcId = g.addMapping(rand.Uint32()%g.NumThreads, edgeEvent.SrcRaw)
	}
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		dstId = g.addMapping(rand.Uint32()%g.NumThreads, edgeEvent.DstRaw)
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementRoundRobin(edgeEvent TopologyEvent[E], undirected bool, fails *int) (srcId uint32, dstId uint32) {
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
