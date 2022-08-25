package main

import (
	"math"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

const DAMPINGFACTOR = float64(0.85)
const INITMASS = 1.0

var EPSILON = float64(0.001)

type VertexProperty struct {
	Residual float64
	Value    float64
}

func MessageAggregator(target *graph.Vertex[VertexProperty], data float64) (newInfo bool) {
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

func AggregateRetrieve(target *graph.Vertex[VertexProperty]) float64 {
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

func OnInitVertex(g *graph.Graph[VertexProperty], vidx uint32) {
	g.Vertices[vidx].Property.Residual = INITMASS
	g.Vertices[vidx].Property.Value = 0.0
	g.Vertices[vidx].Scratch = 0.0
}

// OnEdgeAdd is the complex version which merges a Visit call.
func OnEdgeAdd(g *graph.Graph[VertexProperty], sidx uint32, didxs map[uint32]int, data float64) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Property.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

	src.Property.Residual += data
	toDistribute := DAMPINGFACTOR * (src.Property.Residual)
	toAbsorb := (1.0 - DAMPINGFACTOR) * (src.Property.Residual)
	src.Property.Value += toAbsorb
	src.Property.Residual = 0.0
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
func OnEdgeAddBasic(g *graph.Graph[VertexProperty], sidx uint32, didx uint32, data float64) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Property.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

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

func OnEdgeDel(g *graph.Graph[VertexProperty], sidx uint32, didx uint32, data float64) {
	src := &g.Vertices[sidx]
	distAllPrev := src.Property.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

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

func OnVisitVertex(g *graph.Graph[VertexProperty], vidx uint32, data float64) int {
	vertex := &g.Vertices[vidx]
	vertex.Property.Residual += data

	if math.Abs(vertex.Property.Residual) > EPSILON {
		toDistribute := DAMPINGFACTOR * (vertex.Property.Residual)
		toAbsorb := (1.0 - DAMPINGFACTOR) * (vertex.Property.Residual)

		vertex.Property.Value += toAbsorb
		vertex.Property.Residual = 0.0

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

// OnFinish: Called at the end of the algorithm to finalize anything necessary.
// For pagerank, we use this opportunity to resolve the issue of Sink Vertices.
// The description of the code can be found in the following paper;
// "No More Leaky PageRank", S. Sallinen, M. Ripeanu, published in IA^3
// https://people.ece.ubc.ca/matei/papers/ia3-2021.pdf
//
// A minor modification has been made since the publication of the paper,
// we no longer need to track latent values within a sink during processing, as it can actually be computed at the end
// with simply the computation g.Vertices[vidx].Property.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
func OnFinish(g *graph.Graph[VertexProperty]) error {
	// Fix all sink node latent values
	numSinks := 0       /// Number of sink nodes.
	globalLatent := 0.0 /// Total latent values from sinks.
	nonSinkSum := 0.0   /// The total accumulated value in the non-sink graph.

	// One pass over all vertices -- compute some global totals.
	for vidx := range g.Vertices {
		// New: absorb any leftovers of residual
		//*/
		g.Vertices[vidx].Property.Value += (1.0 - DAMPINGFACTOR) * (g.Vertices[vidx].Property.Residual + g.Vertices[vidx].Scratch)
		// Ideally we distribute (the residual should be spread among nbrs).
		// But we must cheat the total sum mass check, so we leave some here (Residual is no longer meaningful, just used for bookkeeping).
		g.Vertices[vidx].Property.Residual = (DAMPINGFACTOR) * (g.Vertices[vidx].Property.Residual + g.Vertices[vidx].Scratch)
		g.Vertices[vidx].Scratch = 0.0
		//*/

		if len(g.Vertices[vidx].OutEdges) == 0 { // Sink vertex
			globalLatent += g.Vertices[vidx].Property.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
			numSinks++
		} else {
			nonSinkSum += g.Vertices[vidx].Property.Value
		}
	}

	// Note: the amount latent here was already pre-dampened, so the retainment percent must be computed by the raw mass, so we undampen for that calculation (multiply by 1/d).
	// The subtraction of 1.0*sinks is because we discount each sink node's contribution of 1u of mass from the amount latent.
	// We divide by the size of the non-sink graph for the final retainment percent.
	retainSumPct := ((globalLatent * (1.0 / DAMPINGFACTOR)) - 1.0*float64(numSinks)) / float64((len(g.Vertices) - numSinks))

	SinkQuota := float64(1.0) / float64(len(g.Vertices)-1)
	NormalQuota := float64(len(g.Vertices)-numSinks) / float64(len(g.Vertices)-1)

	geometricLatentSum := globalLatent / (1.0 - DAMPINGFACTOR*(SinkQuota*(float64(numSinks-1))+(NormalQuota*retainSumPct)))

	// One pass over all vertices -- make adjustment based on sink/non-sink status.
	for vidx := range g.Vertices {
		var toAbsorb float64
		if len(g.Vertices[vidx].OutEdges) != 0 { // All vertices that are NOT a sink node
			toAbsorb = (NormalQuota) * (geometricLatentSum * (1.0 - retainSumPct)) * (g.Vertices[vidx].Property.Value / nonSinkSum)
		} else { // All vertices that are a sink node
			// Relative 'power' of this sink compared to others determines its retainment. Note: we undampen for this ratio as well.
			vLatent := g.Vertices[vidx].Property.Value * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
			relativeSinkPowerPct := (vLatent*(1.0/DAMPINGFACTOR) - 1.0) / ((globalLatent * (1.0 / DAMPINGFACTOR)) - float64(numSinks)*1.0)
			toAbsorb = (1.0 - DAMPINGFACTOR) * (SinkQuota) * (geometricLatentSum) * (1.0 - vLatent/globalLatent)
			toAbsorb += (1.0 - DAMPINGFACTOR) * (NormalQuota) * (geometricLatentSum) * (retainSumPct) * (relativeSinkPowerPct)
		}
		g.Vertices[vidx].Property.Value += toAbsorb
		// g.Vertices[vidx].Value /= float64(len(g.Vertices)) // If we want to normalize here
	}

	return nil
}
