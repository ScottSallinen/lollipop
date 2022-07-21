package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

const DAMPINGFACTOR = float64(0.85)
const INITMASS = 1.0

var EPSILON = float64(0.001)

func MessageAggregator(target *graph.Vertex, data float64) (newInfo bool) {
	/*
		target.Mutex.Lock()
		tmp := target.Scratch
		target.Scratch += data
		target.Mutex.Unlock()
		return tmp == 0.0
	*/
	old := mathutils.AtomicAddFloat64(&target.Scratch, data)
	return old == 0.0
}

func AggregateRetrieve(target *graph.Vertex) float64 {
	/*
		target.Mutex.Lock()
		tmp := target.Scratch
		target.Scratch = 0
		target.Mutex.Unlock()
		return tmp
	*/
	old := mathutils.AtomicSwapFloat64(&target.Scratch, 0.0)
	return old
}

func OnInitVertex(g *graph.Graph, vidx uint32) {
	g.Vertices[vidx].Residual = INITMASS
	g.Vertices[vidx].Value = 0.0
	g.Vertices[vidx].Scratch = 0.0
}

// OnEdgeAdd is the complex version which merges a Visit call.
func OnEdgeAdd(g *graph.Graph, sidx uint32, didxs map[uint32]int, data float64) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

	src.Residual += data
	toDistribute := DAMPINGFACTOR * (src.Residual)
	toAbsorb := (1.0 - DAMPINGFACTOR) * (src.Residual)
	src.Value += toAbsorb
	src.Residual = 0.0
	distribute := toDistribute / float64(len(src.OutEdges))

	if len(src.OutEdges) > 1 { /// Not just our first edge
		distOld := distAllPrev / (float64(len(src.OutEdges) - len(didxs)))
		distNew := distAllPrev / (float64(len(src.OutEdges)))
		distDelta := distNew - distOld

		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Target
			if _, in := didxs[target]; in {
				/// Do nothing, this only goes to old edges
			} else {
				g.OnQueueVisit(g, sidx, target, distDelta+distribute)
			}
		}
	}
	distNewEdge := distAllPrev / (float64(len(src.OutEdges)))

	for didx := range didxs {
		g.OnQueueVisit(g, sidx, didx, distNewEdge+distribute)
	}
}

// OnEdgeAddBasic is the simple version which does not merge a Visit call.
func OnEdgeAddBasic(g *graph.Graph, sidx uint32, didx uint32, data float64) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

	if len(src.OutEdges) > 1 { /// Not just our first edge
		distOld := distAllPrev / (float64(len(src.OutEdges) - 1))
		distNew := distAllPrev / (float64(len(src.OutEdges)))
		distDelta := distNew - distOld

		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Target
			if target != didx { /// Only old edges
				g.OnQueueVisit(g, sidx, target, distDelta)
			}
		}
	}
	distNewEdge := distAllPrev / (float64(len(src.OutEdges)))

	g.OnQueueVisit(g, sidx, didx, distNewEdge)
}

func OnEdgeDel(g *graph.Graph, sidx uint32, didx uint32, data float64) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

	if len(src.OutEdges) > 0 { /// Still have edges left
		distOld := distAllPrev / (float64(len(src.OutEdges) + 1))
		distNew := distAllPrev / (float64(len(src.OutEdges)))
		distDelta := distNew - distOld

		for eidx := range src.OutEdges {
			target := src.OutEdges[eidx].Target
			g.OnQueueVisit(g, sidx, target, distDelta)
		}
	}
	distOldEdge := -1.0 * distAllPrev / (float64(len(src.OutEdges) + 1))

	g.OnQueueVisit(g, sidx, didx, distOldEdge)
}

func OnVisitVertex(g *graph.Graph, vidx uint32, data float64) int {
	vertex := &g.Vertices[vidx]
	vertex.Residual += data

	if math.Abs(vertex.Residual) > EPSILON {
		toDistribute := DAMPINGFACTOR * (vertex.Residual)
		toAbsorb := (1.0 - DAMPINGFACTOR) * (vertex.Residual)

		vertex.Value += toAbsorb
		vertex.Residual = 0.0

		if len(vertex.OutEdges) > 0 {
			distribute := toDistribute / float64(len(vertex.OutEdges))
			for eidx := range vertex.OutEdges {
				target := vertex.OutEdges[eidx].Target
				g.OnQueueVisit(g, vidx, target, distribute)
			}
		}
		return len(vertex.OutEdges)
	}
	return 0
}

func OnFinish(g *graph.Graph) error {
	/// Fix all sink node latent values
	numSinks := 0       /// Number of sink nodes.
	globalLatent := 0.0 /// Total latent values from sinks.
	nonSinkSum := 0.0   /// The total accumulated value in the non-sink graph.

	/// One pass over all vertices -- compute some global totals.
	for vidx := range g.Vertices {
		// New: absorb any leftovers of residual
		//*/
		g.Vertices[vidx].Value += (1.0 - DAMPINGFACTOR) * (g.Vertices[vidx].Residual + g.Vertices[vidx].Scratch)
		// Ideally we distribute (the residual should be spread among nbrs).
		// But we must cheat the total sum mass check, so we leave some here (Residual is no longer meaningful, just used for bookkeeping).
		g.Vertices[vidx].Residual = (DAMPINGFACTOR) * (g.Vertices[vidx].Residual + g.Vertices[vidx].Scratch)
		g.Vertices[vidx].Scratch = 0.0
		//*/

		if len(g.Vertices[vidx].OutEdges) == 0 { /// Sink vertex
			globalLatent += g.Vertices[vidx].Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
			numSinks++
		} else {
			nonSinkSum += g.Vertices[vidx].Value
		}
	}

	/// Note: the amount latent here was already pre-dampened, so the retainment percent must be computed by the raw mass, so we undampen for that calculation (multiply by 1/d).
	/// The subtraction of 1.0*sinks is because we discount each sink node's contribution of 1u of mass from the amount latent.
	/// We divide by the size of the non-sink graph for the final retainment percent.
	retainSumPct := ((globalLatent * (1.0 / DAMPINGFACTOR)) - 1.0*float64(numSinks)) / float64((len(g.Vertices) - numSinks))

	SinkQuota := float64(1.0) / float64(len(g.Vertices)-1)
	NormalQuota := float64(len(g.Vertices)-numSinks) / float64(len(g.Vertices)-1)

	geometricLatentSum := globalLatent / (1.0 - DAMPINGFACTOR*(SinkQuota*(float64(numSinks-1))+(NormalQuota*retainSumPct)))

	/// One pass over all vertices -- make adjustment based on sink/non-sink status.
	for vidx := range g.Vertices {
		var toAbsorb float64
		if len(g.Vertices[vidx].OutEdges) != 0 { /// All vertices that are NOT a sink node
			toAbsorb = (NormalQuota) * (geometricLatentSum * (1.0 - retainSumPct)) * (g.Vertices[vidx].Value / nonSinkSum)
		} else { /// All vertices that are a sink node
			/// Relative 'power' of this sink compared to others determines its retainment. Note: we undampen for this ratio as well.
			vLatent := g.Vertices[vidx].Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
			relativeSinkPowerPct := (vLatent*(1.0/DAMPINGFACTOR) - 1.0) / ((globalLatent * (1.0 / DAMPINGFACTOR)) - float64(numSinks)*1.0)
			toAbsorb = (1.0 - DAMPINGFACTOR) * (SinkQuota) * (geometricLatentSum) * (1.0 - vLatent/globalLatent)
			toAbsorb += (1.0 - DAMPINGFACTOR) * (NormalQuota) * (geometricLatentSum) * (retainSumPct) * (relativeSinkPowerPct)
		}
		g.Vertices[vidx].Value += toAbsorb
		//g.Vertices[vidx].Value /= float64(len(g.Vertices)) /// If we want to normalize here
	}

	return nil
}
