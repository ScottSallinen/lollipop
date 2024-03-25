package main

import (
	"math"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type PageRank struct{}

const DAMPINGFACTOR = float64(0.85) // Aka "alpha"
const INITMASS = 1.0                // Given per vertex (should be 1, or the sink norm doesn't work right)

const NORMALIZE = true               // Divide scores by the number of vertices at the end, to determine probability.
const NORM_IGNORE_SINGLETONS = false // If NORMALIZE, ignore vertices with no out or in edges (consider them similar to deleted)
const PPR = false

// Epsilon value.. typically these found in literature. Silly that it is not normalized to the number of vertices!
const COMPARATIVE_E = float64(INITMASS * 1e-3) // Default value for democratic PR. Will continue if ANY rank change exceeds this.
// const COMPARATIVE_E = float64(INITMASS * 1e-9) // Default value for PPR. Will continue if ANY rank change exceeds this.

// Our "residual" uses a slightly different model. Galois looks like this:
// -- if (sdata.residual > tolerance):
// ---- PRTy oldResidual = sdata.residual.exchange(0.0);
// ---- sdata.Score += oldResidual;
//
// We follow this 'flow' format (note our flow may also be negative), where each vertex gets an input of 1.0 mass of flow.
// -- if math.Abs(vertex.Property.InFlow) > EPSILON:
// ---- toDistribute := DAMPINGFACTOR * (prop.InFlow)
// ---- toAbsorb := (1.0 - DAMPINGFACTOR) * (prop.InFlow)
// ---- prop.Mass += toAbsorb
//
// So we divide E by (1-d), as in our model, the vertex rank changes by: (1-d) * InFlow. So, if (InFlow > E/(1-d)), then the rank changes by (((1-d) * InFlow) > E).
const EPSILON = float64(COMPARATIVE_E / (1.0 - DAMPINGFACTOR))

type VertexProperty struct {
	InFlow float64
	Mass   float64
}

type EdgeProperty struct {
	graph.WithTimestamp
	graph.NoRaw
	graph.NoWeight
}

type Mail struct {
	Value float64
}

type Note struct{}

func (VertexProperty) New() VertexProperty {
	return VertexProperty{InFlow: 0, Mass: 0}
}

func (Mail) New() Mail {
	return Mail{0}
}

func (*PageRank) InitAllMail(_ *graph.Vertex[VertexProperty, EdgeProperty], _ *VertexProperty, _ uint32, _ graph.RawType) Mail {
	return Mail{INITMASS}
}

func (*PageRank) MailMerge(incoming Mail, _ uint32, existing *Mail) bool {
	oldU, newU := utils.AtomicAddFloat64U(&existing.Value, incoming.Value)
	return math.Float64frombits(oldU&^(1<<63)) < EPSILON && math.Float64frombits(newU&^(1<<63)) > EPSILON
	// Has to be the ugliness above to inline... below for reference of what it is.
	// old := utils.AtomicAddFloat64(&existing.Value, incoming.Value)
	// return math.Abs(old) < EPSILON && math.Abs(old+incoming.Value) > EPSILON
}

func (*PageRank) MailRetrieve(existing *Mail, _ *graph.Vertex[VertexProperty, EdgeProperty], _ *VertexProperty) (outgoing Mail) {
	outgoing.Value = utils.AtomicSwapFloat64(&existing.Value, 0)
	return outgoing
}

func (alg *PageRank) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, notif graph.Notification[Note], m Mail) (sent uint64) {
	prop.InFlow += m.Value

	if math.Abs(prop.InFlow) > EPSILON {
		toDistribute := DAMPINGFACTOR * (prop.InFlow)
		toAbsorb := (1.0 - DAMPINGFACTOR) * (prop.InFlow)

		prop.Mass += toAbsorb
		prop.InFlow = 0.0

		if len(src.OutEdges) > 0 {
			distribute := Mail{(toDistribute / float64(len(src.OutEdges)))}
			for _, e := range src.OutEdges {
				mailbox, tidx := g.NodeVertexMailbox(e.Didx)
				g.UpdateMsgStat(uint32(gt.Tidx), tidx)
				if alg.MailMerge(distribute, notif.Target, &mailbox.Inbox) {
					sent += g.EnsureSend(g.UniqueNotification(notif.Target, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
				}
			}
		}
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *PageRank) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	distAllPrev := prop.Mass * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

	prop.InFlow += m.Value
	toDistribute := DAMPINGFACTOR * (prop.InFlow)
	toAbsorb := (1.0 - DAMPINGFACTOR) * (prop.InFlow)
	prop.Mass += toAbsorb
	prop.InFlow = 0.0
	distribute := toDistribute / float64(len(src.OutEdges))
	distNew := distAllPrev / float64(len(src.OutEdges)) // Current (new) edge count

	if eidxStart != 0 { // Not just our first edge(s)
		distOld := distAllPrev / (float64(eidxStart)) // Previous edge count
		distDelta := distNew - distOld
		prevEGet := Mail{distDelta + distribute}

		// Previously existing edges [0, new) get this adjustment.
		for eidx := 0; eidx < eidxStart; eidx++ {
			mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[eidx].Didx)
			g.UpdateMsgStat(uint32(gt.Tidx), tidx)
			if alg.MailMerge(prevEGet, sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: src.OutEdges[eidx].Didx}, mailbox, tidx))
			}
		}
	}

	newEGet := Mail{distNew + distribute}
	// New edges [new, len) get this adjustment
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		mailbox, tidx := g.NodeVertexMailbox(src.OutEdges[eidx].Didx)
		g.UpdateMsgStat(uint32(gt.Tidx), tidx)
		if alg.MailMerge(newEGet, sidx, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: src.OutEdges[eidx].Didx}, mailbox, tidx))
		}
	}
	return sent
}

// Version that merges with a visit
func (alg *PageRank) OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, deletedEdges []graph.Edge[EdgeProperty], m Mail) (sent uint64) {
	distAllPrev := prop.Mass * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))

	prop.InFlow += m.Value
	toDistribute := DAMPINGFACTOR * (prop.InFlow)
	toAbsorb := (1.0 - DAMPINGFACTOR) * (prop.InFlow)
	prop.Mass += toAbsorb
	prop.InFlow = 0.0

	if len(src.OutEdges) > 0 { // Still have edges left
		distribute := toDistribute / float64(len(src.OutEdges))
		distOld := distAllPrev / (float64(len(src.OutEdges) + len(deletedEdges)))
		distNew := distAllPrev / (float64(len(src.OutEdges)))
		distDelta := distNew - distOld

		for _, e := range src.OutEdges {
			mailbox, tidx := g.NodeVertexMailbox(e.Didx)
			g.UpdateMsgStat(uint32(gt.Tidx), tidx)
			if alg.MailMerge(Mail{distDelta + distribute}, sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
			}
		}
	}

	distOldEdges := -1.0 * distAllPrev / (float64(len(src.OutEdges) + len(deletedEdges)))
	for _, e := range deletedEdges {
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		g.UpdateMsgStat(uint32(gt.Tidx), tidx)
		if alg.MailMerge(Mail{distOldEdges}, sidx, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
		}
	}
	return sent
}

// OnFinish: Called at the end of the algorithm to finalize anything necessary.
// For pagerank, we use this opportunity to resolve the issue of Sink Vertices.
// The description of the code can be found in the following paper;
// "No More Leaky PageRank", S. Sallinen, M. Ripeanu, published in IA^3
// https://people.ece.ubc.ca/matei/papers/ia3-2021.pdf
//
// A minor modification has been made since the publication of the paper,
// we no longer need to track latent values within a sink during processing, as it can actually be computed at the end
// with simply the computation vertex.Property.Mass * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
//
// This function reads properties from gOrigin, and writes the final properties to gWrite.
func (*PageRank) OnFinish(gOrigin *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gWrite *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], AtEvent uint64) {
	// Fix all sink node latent values
	globalLatent := float64(0) // Total latent values from sinks.
	numSinks := int64(0)       // Number of sink nodes.
	numDiscarded := int64(0)   // Number of discarded vertices.
	nonSinkSum := float64(0)   // The total accumulated value in the non-sink graph.

	// One pass over all vertices -- compute some global totals.
	singletons := gWrite.NodeParallelFor(func(_, _ uint32, gWriteT *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		gOriginT := &gOrigin.GraphThreads[gWriteT.Tidx]
		tSingletons := 0
		tNumSinks := 0
		tGlobalLatent := float64(0)
		tNonSinkSum := float64(0)
		tDiscarded := 0

		for i := 0; i < len(gWriteT.Vertices); i++ {
			writeProp := gWriteT.VertexProperty(uint32(i))
			originProp := gOriginT.VertexProperty(uint32(i))
			structure := gWriteT.VertexStructure(uint32(i))
			*writeProp = *originProp // Copy each property

			// Absorb any leftovers of residual
			leftovers := (originProp.InFlow)
			writeProp.Mass += (1.0 - DAMPINGFACTOR) * leftovers
			// Ideally we distribute (the residual should be spread among nbrs). But we must cheat the total sum mass check, so we leave some here (Residual is no longer meaningful, just used for bookkeeping).
			writeProp.InFlow = (DAMPINGFACTOR) * leftovers
			if len(gWriteT.Vertices[i].OutEdges) == 0 { // Sink vertex
				if NORM_IGNORE_SINGLETONS && utils.FloatEquals(writeProp.Mass, (1-DAMPINGFACTOR), 0.001) { // TODO: check edge end timings instead? float equality is bad.
					writeProp.Mass = 0 // discard singleton
					tSingletons++
				} else if structure.CreateEvent > AtEvent { // Vertex would not exist yet. Created by an in-event beyond the query (no in or out edges yet).
					writeProp.Mass = 0 // discard
					tDiscarded++
				} else {
					tGlobalLatent += writeProp.Mass
					tNumSinks++
				}
			} else {
				tNonSinkSum += writeProp.Mass
			}
		}
		atomic.AddInt64(&numSinks, int64(tNumSinks))
		atomic.AddInt64(&numDiscarded, int64(tDiscarded))
		utils.AtomicAddFloat64(&globalLatent, tGlobalLatent)
		utils.AtomicAddFloat64(&nonSinkSum, tNonSinkSum)
		return tSingletons
	})

	globalLatent = globalLatent * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
	nV := int64(gWrite.NodeVertexCount() - singletons - int(numDiscarded))
	// println("Singletons ", singletons, " Discarded ", numDiscarded, " nv ", nV, " numSinks ", numSinks, " globalLatent ", globalLatent)

	// Note: the amount latent here was already pre-dampened, so the retainment percent must be computed by the raw mass, so we undampen for that calculation (multiply by 1/d).
	// The subtraction of 1.0*sinks is because we discount each sink node's contribution of 1u of mass from the amount latent.
	// We divide by the size of the non-sink graph for the final retainment percent.
	retainSumPct := ((globalLatent * (1.0 / DAMPINGFACTOR)) - 1.0*float64(numSinks)) / float64((nV - numSinks))

	SinkQuota := float64(1.0) / float64(nV-1)
	NormalQuota := float64(nV-numSinks) / float64(nV-1)

	geometricLatentSum := globalLatent / (1.0 - DAMPINGFACTOR*(SinkQuota*(float64(numSinks-1))+(NormalQuota*retainSumPct)))

	// One pass over all vertices -- make adjustment based on sink/non-sink status.
	gWrite.NodeParallelFor(func(_, _ uint32, gWriteT *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		for i := 0; i < len(gWriteT.Vertices); i++ {
			writeV := &gWriteT.Vertices[i]
			writeProp := gWriteT.VertexProperty(uint32(i))

			if len(writeV.OutEdges) != 0 { // All vertices that are NOT a sink node
				writeProp.Mass += (NormalQuota) * (geometricLatentSum * (1.0 - retainSumPct)) * (writeProp.Mass / nonSinkSum)
			} else { // All vertices that are a sink node
				if writeProp.Mass == 0 { // Previously discarded
					continue
				}
				// Relative 'power' of this sink compared to others determines its retainment. Note: we undampen for this ratio as well.
				vLatent := writeProp.Mass * (DAMPINGFACTOR / (1.0 - DAMPINGFACTOR))
				relativeSinkPowerPct := (vLatent*(1.0/DAMPINGFACTOR) - 1.0) / ((globalLatent * (1.0 / DAMPINGFACTOR)) - float64(numSinks))
				writeProp.Mass += (1.0 - DAMPINGFACTOR) * (geometricLatentSum) * ((SinkQuota)*(1.0-vLatent/globalLatent) + (NormalQuota)*(retainSumPct)*(relativeSinkPowerPct))
			}
			if NORMALIZE && !PPR {
				writeProp.Mass /= float64(nV)
			}
		}
		return 0
	})
}
