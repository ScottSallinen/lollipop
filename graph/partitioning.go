package graph

import (
	"log"
	"math"
	"math/rand"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/utils"
)

var numVertices uint32 = 0
var numEvents uint64 = 0
var threadVertexCounts = make([]uint64, THREAD_MAX)
var threadOutEdgeCounts = make([]uint64, THREAD_MAX)

const PARTITIONING_BATCH_SIZE = 1

var LOAD_ADJUSTMENT_NONE = func(_ uint32, load float64) float64 {
	return load
}

// Bulk placement

func (g *Graph[V, E, M, N]) FindVertexPlacementBulk(eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], batchNumEvents int, undirected bool) {
	if undirected {
		log.Panic("undirected is not supported")
	}
	g.FindVertexPlacementBulkIndividual(eventBatch, eventBatchPlacement, batchNumEvents, undirected)
}

func (g *Graph[V, E, M, N]) FindVertexPlacementBulkIndividual(eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], batchNumEvents int, undirected bool) {
	for i := 0; i < batchNumEvents; i++ {
		eventBatchPlacement[i].First, eventBatchPlacement[i].Second = g.FindVertexPlacement(eventBatch[i], undirected)
	}
}

func (g *Graph[V, E, M, N]) FindVertexPlacementBulkBetter(eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], batchNumEvents int, undirected bool) {
	// Load balancing is pretty bad with these two parameters?
	numEvents += uint64(batchNumEvents)

	vertices := make(map[RawType][]uint32)
	for i := 0; i < batchNumEvents; i++ {
		srcId, srcOk := g.VertexMap[eventBatch[i].SrcRaw]
		dstId, dstOk := g.VertexMap[eventBatch[i].DstRaw]
		if !srcOk && !dstOk {
			if _, ok := vertices[eventBatch[i].SrcRaw]; !ok {
				vertices[eventBatch[i].SrcRaw] = nil
			}
			if _, ok := vertices[eventBatch[i].DstRaw]; !ok {
				vertices[eventBatch[i].DstRaw] = nil
			}
		} else if srcOk && !dstOk {
			vertices[eventBatch[i].DstRaw] = append(vertices[eventBatch[i].DstRaw], IdxToTidx(srcId))
		} else if !srcOk && dstOk {
			vertices[eventBatch[i].SrcRaw] = append(vertices[eventBatch[i].SrcRaw], IdxToTidx(dstId))
		}
	}

	threadLoads := make([]float64, g.NumThreads)
	threadCommons := make([]float64, g.NumThreads)
	avgLoad := g.getAvgLoad()
	for t := 0; t < int(g.NumThreads); t++ {
		threadLoads[t] = g.GraphThreads[t].GetLoad() / avgLoad
	}
	for vRaw, nbrs := range vertices {
		for t := range threadCommons {
			threadCommons[t] = 0
		}
		for _, nbr := range nbrs {
			threadCommons[nbr] += 1 // Explore
		}

		minTidx, minLoad := uint32(0), math.MaxFloat64
		for t := uint32(0); t < g.NumThreads; t++ {
			load := threadLoads[t] - threadCommons[t]
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
		threadOutEdgeCounts[IdxToTidx(dstId)] += 1
		eventBatchPlacement[i].First, eventBatchPlacement[i].Second = srcId, dstId
	}
}

func (g *Graph[V, E, M, N]) FindVertexPlacementBulkFennel(eventBatch []TopologyEvent[E], eventBatchPlacement []utils.Pair[uint32, uint32], batchNumEvents int, undirected bool) {
	// Load balancing is pretty bad with these two parameters?
	numEvents += uint64(batchNumEvents)
	alpha := math.Sqrt(float64(g.NumThreads)) * float64(numEvents) / math.Pow(float64(numVertices), 1.5)
	gamma := 1.5

	vertices := make(map[RawType][]uint32)
	for i := 0; i < batchNumEvents; i++ {
		srcId, srcOk := g.VertexMap[eventBatch[i].SrcRaw]
		dstId, dstOk := g.VertexMap[eventBatch[i].DstRaw]
		if !srcOk && !dstOk {
			if _, ok := vertices[eventBatch[i].SrcRaw]; !ok {
				vertices[eventBatch[i].SrcRaw] = nil
			}
			if _, ok := vertices[eventBatch[i].DstRaw]; !ok {
				vertices[eventBatch[i].DstRaw] = nil
			}
		} else if srcOk && !dstOk {
			vertices[eventBatch[i].DstRaw] = append(vertices[eventBatch[i].DstRaw], IdxToTidx(srcId))
		} else if !srcOk && dstOk {
			vertices[eventBatch[i].SrcRaw] = append(vertices[eventBatch[i].SrcRaw], IdxToTidx(dstId))
		}
	}

	threadLoads := make([]float64, g.NumThreads)
	threadCommons := make([]float64, g.NumThreads)
	for t := 0; t < int(g.NumThreads); t++ {
		threadLoads[t] = alpha * gamma * math.Pow(g.GraphThreads[t].GetLoad(), gamma-1)
	}
	for vRaw, nbrs := range vertices {
		for t := range threadCommons {
			threadCommons[t] = 0
		}
		for _, nbr := range nbrs {
			threadCommons[nbr] += 1
		}

		minTidx, minLoad := uint32(0), math.MaxFloat64
		for t := uint32(0); t < g.NumThreads; t++ {
			load := threadLoads[t] - threadCommons[t]
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
		threadOutEdgeCounts[IdxToTidx(dstId)] += 1
		eventBatchPlacement[i].First, eventBatchPlacement[i].Second = srcId, dstId
	}
}

// Individual placement

func (g *Graph[V, E, M, N]) FindVertexPlacement(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	srcId, dstId = g.FindVertexPlacementBetter(edgeEvent, undirected)
	threadOutEdgeCounts[IdxToTidx(srcId)] += 1
	return
}

// Complex ones

func (g *Graph[V, E, M, N]) FindVertexPlacementMinLoad(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	srcId, srcOk := g.VertexMap[edgeEvent.SrcRaw]
	dstId, dstOk := g.VertexMap[edgeEvent.DstRaw]
	if !srcOk && !dstOk {
		// min load thread
		tMinLoad := g.findMinLoad(LOAD_ADJUSTMENT_NONE)
		srcId = g.addMapping(tMinLoad, edgeEvent.SrcRaw)
		dstId = g.addMapping(tMinLoad, edgeEvent.DstRaw)
	} else if srcOk != dstOk {
		if srcOk {
			tMinLoad := g.findMinLoad(LOAD_ADJUSTMENT_NONE)
			dstId = g.addMapping(tMinLoad, edgeEvent.DstRaw)
		} else {
			tMinLoad := g.findMinLoad(LOAD_ADJUSTMENT_NONE)
			srcId = g.addMapping(tMinLoad, edgeEvent.SrcRaw)
		}
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementBetter(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	srcId, srcOk := g.VertexMap[edgeEvent.SrcRaw]
	dstId, dstOk := g.VertexMap[edgeEvent.DstRaw]
	if !srcOk && !dstOk {
		// min load thread
		tMinLoad := g.findMinLoad(LOAD_ADJUSTMENT_NONE)
		srcId = g.addMapping(tMinLoad, edgeEvent.SrcRaw)
		dstId = g.addMapping(tMinLoad, edgeEvent.DstRaw)
	} else if srcOk != dstOk {
		avgLoad := g.getAvgLoad()
		if srcOk {
			srcTidx := IdxToTidx(srcId)
			tMinLoad := g.findMinLoad(func(tidx uint32, load float64) float64 {
				load /= avgLoad
				if tidx == srcTidx {
					load -= 1 // Explore
				}
				return load
			})
			dstId = g.addMapping(tMinLoad, edgeEvent.DstRaw)
		} else {
			dstTidx := IdxToTidx(dstId)
			tMinLoad := g.findMinLoad(func(tidx uint32, load float64) float64 {
				load /= avgLoad
				if tidx == dstTidx {
					load -= 1 // Explore
				}
				return load
			})
			srcId = g.addMapping(tMinLoad, edgeEvent.SrcRaw)
		}
	}
	return
}

func (g *Graph[V, E, M, N]) FindVertexPlacementFennelLike(edgeEvent TopologyEvent[E], undirected bool) (srcId uint32, dstId uint32) {
	// Load balancing is pretty bad with these two parameters
	numEvents += 1
	alpha := math.Sqrt(float64(g.NumThreads)) * float64(numEvents) / math.Pow(float64(numVertices), 1.5)
	gamma := 1.5

	srcId, srcOk := g.VertexMap[edgeEvent.SrcRaw]
	dstId, dstOk := g.VertexMap[edgeEvent.DstRaw]
	if !srcOk && !dstOk {
		// min load thread
		tMinLoad := g.findMinLoad(LOAD_ADJUSTMENT_NONE)
		srcId = g.addMapping(tMinLoad, edgeEvent.SrcRaw)
		dstId = g.addMapping(tMinLoad, edgeEvent.DstRaw)
	} else if srcOk != dstOk {
		if srcOk {
			srcTidx := IdxToTidx(srcId)
			tMinLoad := g.findMinLoad(func(tidx uint32, load float64) float64 {
				load = alpha * gamma * math.Pow(load, gamma-1)
				if tidx == srcTidx {
					load -= 1
				}
				return load
			})
			dstId = g.addMapping(tMinLoad, edgeEvent.DstRaw)
		} else {
			dstTidx := IdxToTidx(dstId)
			tMinLoad := g.findMinLoad(func(tidx uint32, load float64) float64 {
				load = alpha * gamma * math.Pow(load, gamma-1)
				if tidx == dstTidx {
					load -= 1
				}
				return load
			})
			srcId = g.addMapping(tMinLoad, edgeEvent.SrcRaw)
		}
	}
	return
}

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

// Load functions

func (gt *GraphThread[V, E, M, N]) GetLoad() (load float64) {
	// TODO: considering mixing different criteria
	return gt.getLoadNumEdges() // Explore
}

func (gt *GraphThread[V, E, M, N]) getLoadNumVertices() (load float64) {
	return float64(threadVertexCounts[gt.Tidx])
}

func (gt *GraphThread[V, E, M, N]) getLoadNumEdges() (load float64) {
	return float64(threadOutEdgeCounts[gt.Tidx])
}

func (gt *GraphThread[V, E, M, N]) getLoadNumMessages() (load float64) {
	return float64(atomic.LoadUint64(&gt.MsgRecvLocal) + atomic.LoadUint64(&gt.MsgRecvRemote))
}

func (g *Graph[V, E, M, N]) findMinLoad(adjustLoad func(uint32, float64) float64) (tMinLoad uint32) {
	minLoad := math.MaxFloat64
	for t := uint32(0); t < g.NumThreads; t++ {
		load := adjustLoad(t, g.GraphThreads[t].GetLoad())
		if load < minLoad {
			minLoad, tMinLoad = load, t
		}
	}
	return tMinLoad
}

func (g *Graph[V, E, M, N]) getAvgLoad() (avgLoad float64) {
	for t := uint32(0); t < g.NumThreads; t++ {
		avgLoad += g.GraphThreads[t].GetLoad()
	}
	return avgLoad / float64(g.NumThreads)
}
