package graph

func (g *Graph[V, E, M, N]) FindVertexPlacement(edgeEvent TopologyEvent[E], undirected bool, fails *int) (srcId uint32, dstId uint32) {
	var ok bool

	if srcId, ok = g.VertexMap[edgeEvent.SrcRaw]; !ok {
		tidx := edgeEvent.SrcRaw.Integer() % g.NumThreads // TODO: Use a better placement
		srcId = InternalCompress(tidx, g.ThreadVertexCounts[tidx])
		g.ThreadVertexCounts[tidx] += 1
		g.VertexMap[edgeEvent.SrcRaw] = srcId
	}

	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		tidx := edgeEvent.DstRaw.Integer() % g.NumThreads // TODO: Use a better placement
		dstId = InternalCompress(tidx, g.ThreadVertexCounts[tidx])
		g.ThreadVertexCounts[tidx] += 1
		g.VertexMap[edgeEvent.DstRaw] = dstId
	}

	return
}
