package a

import (
	"fmt"
	"math"
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type PushRelabelAgg struct{}

type NoteAgg struct{}

type VPropAgg struct {
	Type   VertexType
	Height int64
	Excess int32
	ResCap map[uint32]int32

	OutList []utils.Pair[uint32, int32]
}

type EPropAgg struct {
	graph.TimestampWeightedEdge
}

type MessageAgg struct {
	//                                      purpose: message, message box, retrieved message
	Height            int64                       // height of sender, -
	Flow              int32                       // flow, -
	Init              bool                        //
	NewMaxVertexCount bool                        // source needs to increase height
	IdFlowPairs       []utils.Pair[uint32, int32] // -, id flow pairs (0: new neighbour) // TODO: use a SPSC queue?
	HeightCheckList   []uint32                    //
	NbrHeights        map[uint32]int64            // -, heights of neighbours // TODO: make this a multi threaded map so there's no lock when there is no flow
	Mutex             *sync.Mutex                 // TODO: replace with RWMutex
}

func (VPropAgg) New() (new VPropAgg) {
	new.ResCap = make(map[uint32]int32)
	return new
}

func (MessageAgg) New() (new MessageAgg) {
	new.NbrHeights = make(map[uint32]int64)
	return new
}

func RunAggH(options graph.GraphOptions) (maxFlow int32, g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg]) {
	alg := new(PushRelabelAgg)
	g = graph.LaunchGraphExecution[*EPropAgg, VPropAgg, EPropAgg, MessageAgg, NoteAgg](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func (pr *PushRelabelAgg) GetMaxFlowValue(g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg]) int32 {
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabelAgg) InitAllMessage(vertex *graph.Vertex[VPropAgg, EPropAgg], internalId uint32, rawId graph.RawType) MessageAgg {
	return MessageAgg{Init: true}
}

func (pr *PushRelabelAgg) BaseVertexMessage(v *graph.Vertex[VPropAgg, EPropAgg], internalId uint32, rawId graph.RawType) (m MessageAgg) {
	m.Mutex = &sync.Mutex{}
	m.NbrHeights = make(map[uint32]int64)

	v.Property.Height = InitialHeight
	v.Property.ResCap = make(map[uint32]int32)

	if rawId == SourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
	} else if rawId == SinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
	}

	// Iterate over existing edges
	for _, e := range v.OutEdges {
		if e.Didx == internalId || e.Property.Weight <= 0 { // TODO: also skip if the target is source
			continue
		}

		_, isOldNbr := m.NbrHeights[e.Didx]
		if !isOldNbr {
			m.IdFlowPairs = append(m.IdFlowPairs, utils.Pair[uint32, int32]{First: e.Didx, Second: 0})
			m.NbrHeights[e.Didx] = InitialHeight
		}

		v.Property.ResCap[e.Didx] += int32(e.Property.Weight)
		m.HeightCheckList = append(m.HeightCheckList, e.Didx)

		if v.Property.Type == Source {
			v.Property.Excess += int32(e.Property.Weight)
		}
	}

	return m
}

func (*PushRelabelAgg) MessageMerge(incoming MessageAgg, sidx uint32, existing *MessageAgg) (newInfo bool) {
	existing.Mutex.Lock()

	if incoming.Init {
		existing.Init = true
		newInfo = true
	} else {
		oldHeight, isOldNbr := existing.NbrHeights[sidx]
		existing.NbrHeights[sidx] = incoming.Height
		if incoming.NewMaxVertexCount {
			existing.NewMaxVertexCount = true
			newInfo = true
		}
		if incoming.Flow != 0 {
			existing.IdFlowPairs = append(existing.IdFlowPairs, utils.Pair[uint32, int32]{First: sidx, Second: incoming.Flow})
			newInfo = true
		}
		if !isOldNbr {
			existing.IdFlowPairs = append(existing.IdFlowPairs, utils.Pair[uint32, int32]{First: sidx, Second: 0}) // mark as a new neighbour
			newInfo = true
		}
		if !isOldNbr || oldHeight > incoming.Height || incoming.Flow > 0 {
			existing.HeightCheckList = append(existing.HeightCheckList, sidx)
			newInfo = true
		}
	}

	existing.Mutex.Unlock()
	return newInfo
}

func (*PushRelabelAgg) MessageRetrieve(existing *MessageAgg, vertex *graph.Vertex[VPropAgg, EPropAgg]) (outgoing MessageAgg) {
	existing.Mutex.Lock()
	outgoing = *existing
	// Clear msgBox
	existing.Init = false
	existing.NewMaxVertexCount = false
	existing.IdFlowPairs = existing.IdFlowPairs[:0]
	existing.HeightCheckList = existing.HeightCheckList[:0]
	return outgoing
}

func (*PushRelabelAgg) ComputeOutListAndNewHeight(v *VPropAgg, msgBox *MessageAgg) (newHeight int64) {
	newHeight = v.Height
	if msgBox.NewMaxVertexCount {
		if v.Type != Source {
			log.Panic().Msg("Non-source received NewMaxVertexCount")
		}
		newHeight = VertexCountHelper.GetMaxVertexCount()
	}

	// handleFlow
	for _, idFlowPair := range msgBox.IdFlowPairs {
		id, flow := idFlowPair.First, idFlowPair.Second
		if flow < 0 {
			// retract request
			amount := utils.Max(flow, -v.ResCap[id])
			if amount < 0 {
				v.ResCap[id] -= -amount
				v.Excess -= -amount
				v.OutList = append(v.OutList, utils.Pair[uint32, int32]{First: id, Second: -amount})
			}
		} else if flow > 0 {
			// additional flow
			v.ResCap[id] += flow
			v.Excess += flow
		} else { // flow == 0
			// new neighbour
			v.ResCap[id] += 0 // ensure it has an entry in v.ResCap
			v.OutList = append(v.OutList, utils.Pair[uint32, int32]{First: id, Second: 0})
		}
	}

	// restoreHeightInvariant
	for _, id := range msgBox.HeightCheckList {
		// height decreased
		maxHeight, resCap := msgBox.NbrHeights[id]+1, v.ResCap[id]
		if maxHeight < newHeight && resCap > 0 {
			if v.Excess > 0 {
				amount := utils.Min(v.Excess, resCap)
				v.Excess -= amount
				resCap -= amount
				v.OutList = append(v.OutList, utils.Pair[uint32, int32]{First: id, Second: amount})
				v.ResCap[id] = resCap
			}
			if resCap > 0 {
				Assert(v.Type != Source, "")
				newHeight = maxHeight
			}
		}
	}

	if v.Excess > 0 {
		// discharge
		if v.Type == Normal {
		excessFor:
			for v.Excess > 0 {
				nextHeight := int64(MaxHeight)
				for n, rc := range v.ResCap {
					if rc > 0 {
						h := msgBox.NbrHeights[n]
						if !(newHeight > h) {
							nextHeight = utils.Min(nextHeight, h+1)
							continue
						}
						amount := utils.Min(v.Excess, rc)
						v.Excess -= amount
						v.ResCap[n] -= amount
						Assert(amount > 0, "")
						v.OutList = append(v.OutList, utils.Pair[uint32, int32]{First: n, Second: amount})
						if v.Excess == 0 {
							break excessFor
						}
					}
				}
				Assert(nextHeight != MaxHeight, "")
				newHeight = nextHeight
			}
		} else {
			// Cannot lift
			for n, rc := range v.ResCap {
				if rc > 0 && newHeight > msgBox.NbrHeights[n] {
					amount := utils.Min(v.Excess, rc)
					v.Excess -= amount
					v.ResCap[n] -= amount
					v.OutList = append(v.OutList, utils.Pair[uint32, int32]{First: n, Second: amount})
					if v.Excess == 0 {
						break
					}
				}
			}
		}
	} else if v.Excess < 0 && v.Type == Normal && v.Height > 0 {
		newHeight = -VertexCountHelper.GetMaxVertexCount()
	}
	return
}

func (pr *PushRelabelAgg) OnUpdateVertex(g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg], v *graph.Vertex[VPropAgg, EPropAgg], n graph.Notification[NoteAgg], m MessageAgg) (sent uint64) {
	newHeight := pr.ComputeOutListAndNewHeight(&v.Property, &m)
	m.Mutex.Unlock() // NoteAgg: shouldn't call MessageMerge before this - could result in deadlocks

	if m.Init {
		source := VertexCountHelper.NewVertex()
		if source != math.MaxUint32 {
			vtm, tidx := g.NodeVertexMessages(source)
			if pr.MessageMerge(MessageAgg{Height: v.Property.Height, NewMaxVertexCount: true}, n.Target, &vtm.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[NoteAgg]{Target: source}, vtm, tidx))
			}
		}
	}
	if newHeight != v.Property.Height {
		// broadcast heights
		msg := MessageAgg{Height: newHeight}
		for nbrId := range v.Property.ResCap {
			vtm, tidx := g.NodeVertexMessages(nbrId)
			if pr.MessageMerge(msg, n.Target, &vtm.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[NoteAgg]{Target: nbrId}, vtm, tidx))
			}
		}
		v.Property.Height = newHeight
	}
	for _, idAndFlow := range v.Property.OutList {
		msg := MessageAgg{Height: newHeight, Flow: idAndFlow.Second}
		vtm, tidx := g.NodeVertexMessages(idAndFlow.First)
		if pr.MessageMerge(msg, n.Target, &vtm.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[NoteAgg]{Target: idAndFlow.First}, vtm, tidx))
		}
	}
	v.Property.OutList = v.Property.OutList[:0]

	return
}

func (pr *PushRelabelAgg) OnEdgeAdd(g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg], src *graph.Vertex[VPropAgg, EPropAgg], sidx uint32, eidxStart int, m MessageAgg) (sent uint64) {
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		e := &src.OutEdges[eidx]
		if e.Didx == sidx || e.Property.Weight <= 0 { // TODO: also skip if the target is source
			continue
		}

		_, isOldNbr := m.NbrHeights[e.Didx]
		if !isOldNbr {
			m.IdFlowPairs = append(m.IdFlowPairs, utils.Pair[uint32, int32]{First: e.Didx, Second: 0})
			m.NbrHeights[e.Didx] = InitialHeight
		}

		src.Property.ResCap[e.Didx] += int32(e.Property.Weight)
		m.HeightCheckList = append(m.HeightCheckList, e.Didx)

		if src.Property.Type == Source {
			src.Property.Excess += int32(e.Property.Weight)
		}
	}

	sent += pr.OnUpdateVertex(g, src, graph.Notification[NoteAgg]{Target: sidx}, m)

	return sent
}

func (*PushRelabelAgg) OnEdgeDel(g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg], src *graph.Vertex[VPropAgg, EPropAgg], sidx uint32, deletedEdges []graph.Edge[EPropAgg], m MessageAgg) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabelAgg) OnCheckCorrectness(g *graph.Graph[VPropAgg, EPropAgg, MessageAgg, NoteAgg]) {
	log.Info().Msg("Ensuring the vertex type is correct")
	sourceInternalId, source := g.NodeVertexFromRaw(SourceRawId)
	sinkInternalId, sink := g.NodeVertexFromRaw(SinkRawId)
	Assert(source.Property.Type == Source, "")
	Assert(sink.Property.Type == Sink, "")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropAgg, EPropAgg]) {
		if v.Property.Type != Normal {
			Assert(internalId == sourceInternalId || internalId == sinkInternalId, "")
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropAgg, EPropAgg]) {
		Assert(len(v.Property.OutList) == 0, "")
		vtm, _ := g.NodeVertexMessages(internalId)
		Assert(vtm.Inbox.Height == 0, "")
		Assert(vtm.Inbox.Flow == 0, "")
		Assert(vtm.Inbox.Init == false, "")
		Assert(vtm.Inbox.NewMaxVertexCount == false, "")
		Assert(len(vtm.Inbox.IdFlowPairs) == 0, "")
		Assert(len(vtm.Inbox.HeightCheckList) == 0, "")

		Assert(len(v.Property.ResCap) == len(vtm.Inbox.NbrHeights), "")
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	Assert(source.Property.Height >= int64(vertexCount),
		fmt.Sprintf("Source height %d < # of vertices %d", source.Property.Height, vertexCount))
	Assert(sink.Property.Height == 0,
		fmt.Sprintf("Sink height %d != 0", sink.Property.Height))

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropAgg, EPropAgg]) {
		if v.Property.Type == Normal {
			Assert(v.Property.Excess == 0, "")
		}
		for n, rc := range v.Property.ResCap {
			Assert(rc >= 0, fmt.Sprintf("%d -> %d has negative residual capacity (%d)", internalId, n, rc))
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropAgg, EPropAgg]) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight == 0 { // TODO: also skip if the target is source
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}
		for _, rc := range v.Property.ResCap {
			capacityResidual += int64(rc)
		}
		if v.Property.Type == Source {
			Assert(int64(v.Property.Excess) == capacityResidual, "")
		} else if v.Property.Type == Sink {
			Assert(capacityOriginal+int64(v.Property.Excess) == capacityResidual, "")
		} else {
			Assert(capacityOriginal == capacityResidual, "")
		}
	})

	log.Info().Msg("Checking sourceOut and sinkIn")
	sourceOut := int64(0)
	for _, e := range source.OutEdges {
		sourceOut += int64(e.Property.Weight)
	}
	sourceOut -= int64(source.Property.Excess)
	sinkIn := int64(sink.Property.Excess)
	Assert(sourceOut == sinkIn, "")
	log.Info().Msg(fmt.Sprintf("Maximum flow from %d to %d is %d", SourceRawId, SinkRawId, sourceOut))

	log.Info().Msg("Ensuring NbrHeight is accurate")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropAgg, EPropAgg]) {
		vtm, _ := g.NodeVertexMessages(internalId)
		for n, h := range vtm.Inbox.NbrHeights {
			realHeight := g.NodeVertex(n).Property.Height
			Assert(h == realHeight, "")
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *graph.Vertex[VPropAgg, EPropAgg]) {
		vtm, _ := g.NodeVertexMessages(internalId)
		h := v.Property.Height
		for n, rc := range v.Property.ResCap {
			if rc > 0 {
				Assert(h <= vtm.Inbox.NbrHeights[n]+1, "")
			}
		}
	})

	// TODO: Print # of vertices in flow
}
