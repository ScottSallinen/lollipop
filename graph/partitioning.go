package graph

import (
	"math"
	"math/rand"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/utils"
)

var numVertices uint32 = 0
var numEvents uint64 = 0
var threadVertexCounts = make([]uint64, THREAD_MAX)
var threadOutEdgeCounts = make([]uint64, THREAD_MAX)

// Simple ones

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
	if dstId, ok = g.VertexMap[edgeEvent.DstRaw]; !ok {
		dstId = g.addMapping(numVertices%g.NumThreads, edgeEvent.DstRaw)

	}
	return
}

func (g *Graph[V, E, M, N]) addMapping(tidx uint32, rawId RawType) (internalId uint32) {
	internalId = InternalCompress(tidx, g.ThreadVertexCounts[tidx])
	g.ThreadVertexCounts[tidx] += 1
	g.VertexMap[rawId] = internalId
	numVertices += 1
	threadVertexCounts[tidx] += 1
	return internalId
}

// Ones to test

func getAvgLoad[V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N]) float64 {
	avgLoad := float64(0)
	for t := uint32(0); t < g.NumThreads; t++ {
		avgLoad += g.Load(&g.GraphThreads[t])
	}
	return avgLoad / float64(g.NumThreads)
}

func LoadV[V VPI[V], E EPI[E], M MVI[M], N any](gt *GraphThread[V, E, M, N]) float64 {
	return float64(threadVertexCounts[gt.Tidx])
}

func LoadE[V VPI[V], E EPI[E], M MVI[M], N any](gt *GraphThread[V, E, M, N]) float64 {
	return float64(threadOutEdgeCounts[gt.Tidx])
}

func LoadMsg[V VPI[V], E EPI[E], M MVI[M], N any](gt *GraphThread[V, E, M, N]) float64 {
	load := float64(atomic.LoadUint64(&gt.MsgRecvLocal) + atomic.LoadUint64(&gt.MsgRecvRemote))
	load += 0.01 * LoadE(gt)
	return load
}

func PartitionerModulo[V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], batchNumEvents int, undirected bool) {
	for i := 0; i < batchNumEvents; i++ {
		var ok bool
		var srcId, dstId uint32
		if srcId, ok = g.VertexMap[eventBatch[i].SrcRaw]; !ok {
			tidx := eventBatch[i].SrcRaw.Integer() % g.NumThreads
			srcId = g.addMapping(tidx, eventBatch[i].SrcRaw)
		}
		if dstId, ok = g.VertexMap[eventBatch[i].DstRaw]; !ok {
			tidx := eventBatch[i].DstRaw.Integer() % g.NumThreads
			dstId = g.addMapping(tidx, eventBatch[i].DstRaw)
		}
		eventBatchPlacement[i].First, eventBatchPlacement[i].Second = srcId, dstId
	}
}

func PartitionerMla[V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], batchNumEvents int, undirected bool) {
	if undirected {
		panic("not supported")
	}

	// Load balancing is pretty bad with these two parameters?
	numEvents += uint64(batchNumEvents)

	vertices := make(map[RawType][]RawType) // Map from (vertex that need to be partitioned) to (the vertex's neighbours' RawIDs)
	for i := 0; i < batchNumEvents; i++ {
		srcRaw, dstRaw := eventBatch[i].SrcRaw, eventBatch[i].DstRaw
		_, srcOk := g.VertexMap[srcRaw]
		_, dstOk := g.VertexMap[dstRaw]
		if !srcOk {
			vertices[srcRaw] = append(vertices[srcRaw], dstRaw)
		}
		if !dstOk {
			vertices[dstRaw] = append(vertices[dstRaw], srcRaw)
		}
	}

	threadLoads := make([]float64, g.NumThreads)
	threadCommons := make([]float64, g.NumThreads)
	for t := 0; t < int(g.NumThreads); t++ {
		threadLoads[t] = g.Load(&g.GraphThreads[t]) / getAvgLoad(g)
	}
	for vRaw, nbrs := range vertices {
		for t := range threadCommons {
			threadCommons[t] = 0
		}
		for _, nbrRaw := range nbrs {
			if nbrId, ok := g.VertexMap[nbrRaw]; ok {
				threadCommons[IdxToTidx(nbrId)] += 1
			}
		}
		minTidx, minLoad := uint32(0), math.MaxFloat64
		for t := uint32(0); t < g.NumThreads; t++ {
			load := threadLoads[t] - g.Options.MlaAlpha*threadCommons[t]
			if load < minLoad {
				minTidx, minLoad = t, load
			}
		}
		g.addMapping(minTidx, vRaw)
	}

	for i := 0; i < batchNumEvents; i++ {
		srcId, ok := g.VertexMap[eventBatch[i].SrcRaw]
		if !ok {
			panic("")
		}
		dstId, ok := g.VertexMap[eventBatch[i].DstRaw]
		if !ok {
			panic("")
		}
		threadOutEdgeCounts[IdxToTidx(srcId)] += 1
		eventBatchPlacement[i].First, eventBatchPlacement[i].Second = srcId, dstId
	}
}
