package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
)

const DAMPINGFACTOR = float64(0.85)
const INITMASS = 1.0

var EPSILON = float64(0.001)

func OnInitVertex(g *graph.Graph, vidx uint32, data interface{}) {
	g.Vertices[vidx].Properties.Residual = INITMASS
	g.Vertices[vidx].Properties.Value = 0.0
	g.Vertices[vidx].Scratch = 0.0
}

func OnEdgeAdd(g *graph.Graph, sidx uint32, didx uint32, data interface{}) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Properties.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

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

func OnEdgeDel(g *graph.Graph, sidx uint32, didx uint32, data interface{}) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Properties.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

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

func OnVisitVertex(g *graph.Graph, vidx uint32, data interface{}) int {
	vertex := &g.Vertices[vidx]
	vertex.Properties.Residual += data.(float64)

	toDistribute := DAMPINGFACTOR * (vertex.Properties.Residual)
	toAbsorb := (1.0 - DAMPINGFACTOR) * (vertex.Properties.Residual)

	/// TODO: Epsilon adjustments...
	if math.Abs(toAbsorb/(vertex.Properties.Value-toAbsorb)) > EPSILON || vertex.Properties.Residual > EPSILON {
		vertex.Properties.Value += toAbsorb
		vertex.Properties.Residual = 0.0

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

func OnFinish(g *graph.Graph, data interface{}) error {
	/// Fix all sink node latent values
	numSinks := 0       /// Number of sink nodes.
	globalLatent := 0.0 /// Total latent values from sinks.
	nonSinkSum := 0.0   /// The total accumulated value in the non-sink graph.

	/// One pass over all vertices -- compute some global totals.
	for vidx := range g.Vertices {
		if len(g.Vertices[vidx].OutEdges) == 0 { /// Sink vertex
			globalLatent += g.Vertices[vidx].Properties.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
			numSinks++
		} else {
			nonSinkSum += g.Vertices[vidx].Properties.Value
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
			toAbsorb = (NormalQuota) * (geometricLatentSum * (1.0 - retainSumPct)) * (g.Vertices[vidx].Properties.Value / nonSinkSum)
		} else { /// All vertices that are a sink node
			/// Relative 'power' of this sink compared to others determines its retainment. Note: we undampen for this ratio as well.
			vLatent := g.Vertices[vidx].Properties.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
			relativeSinkPowerPct := (vLatent*(1.0/DAMPINGFACTOR) - 1.0) / ((globalLatent * (1.0 / DAMPINGFACTOR)) - float64(numSinks)*1.0)
			toAbsorb = (1.0 - DAMPINGFACTOR) * (SinkQuota) * (geometricLatentSum) * (1.0 - vLatent/globalLatent)
			toAbsorb += (1.0 - DAMPINGFACTOR) * (NormalQuota) * (geometricLatentSum) * (retainSumPct) * (relativeSinkPowerPct)
		}
		g.Vertices[vidx].Properties.Value += toAbsorb
		//g.Vertices[vidx].Properties.Value /= float64(len(g.Vertices)) /// If we want to normalize here
	}

	return nil
}
