package i

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"

	. "github.com/ScottSallinen/lollipop/cmd/lp-push-relabel/common"
)

type PushRelabel struct{}

type Neighbour struct {
	Height    int64
	ResCapOut int32
	ResCapIn  int32
	Pos       int32 // Position of me in the neighbour, -1 means unknown
	Didx      uint32
}

type VertexProp struct {
	Type          VertexType
	Excess        int32
	Height        int64
	HeightChanged bool

	Nbrs            []Neighbour
	NbrMap          map[uint32]int32 // Id -> Pos
	UnknownPosCount uint32           // shouldn't do anything if it's not 0
}

type EdgeProp struct {
	graph.WeightedEdge
}

type Message struct{}

type Note struct {
	Height       int64
	Flow         int32 // Could be used for my position when hand-shaking
	ResCapOffset int32 // Use this only for new/deleted edges

	SrcId  uint32 // FIXME: better if this is given by the framework
	SrcPos int32  // position of the sender in the receiver's InNbrs or OutEdges, -1 means unknown

	Handshake         bool
	NewMaxVertexCount bool // source needs to increase height, should not interpret other fields if this is set to true
}

type Graph = graph.Graph[VertexProp, EdgeProp, Message, Note]
type Vertex = graph.Vertex[VertexProp, EdgeProp]
type Edge = graph.Edge[EdgeProp]

const (
	Name = "PushRelabel (I)"
)

func (VertexProp) New() (new VertexProp) {
	new.NbrMap = make(map[uint32]int32)
	return new
}

func (Message) New() (new Message) {
	return new
}

func Run(options graph.GraphOptions) (maxFlow int32, g *Graph) {
	alg := new(PushRelabel)
	g = graph.LaunchGraphExecution[*EdgeProp, VertexProp, EdgeProp, Message, Note](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func (pr *PushRelabel) GetMaxFlowValue(g *Graph) int32 {
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabel) InitAllMessage(vertex *Vertex, internalId uint32, rawId graph.RawType) Message {
	return Message{}
}

func (pr *PushRelabel) BaseVertexMessage(v *Vertex, internalId uint32, rawId graph.RawType) (m Message) {
	v.Property.Height = InitialHeight
	if rawId == SourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
	} else if rawId == SinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
	}
	v.Property.UnknownPosCount = math.MaxUint32 // Make as uninitialized
	return m
}

func (*PushRelabel) MessageMerge(incoming Message, sidx uint32, existing *Message) (newInfo bool) {
	return true
}

func (*PushRelabel) MessageRetrieve(existing *Message, vertex *Vertex) (outgoing Message) {
	return outgoing
}

func (pr *PushRelabel) Init(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// Iterate over existing edges
	Assert(len(v.Property.NbrMap) == 0, "")
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Didx == myId || e.Property.Weight <= 0 { // TODO: also skip if the target is source
			continue
		}
		if v.Property.Type == Source {
			v.Property.Excess += int32(e.Property.Weight)
		}

		pos, exist := v.Property.NbrMap[e.Didx]
		if !exist {
			pos = int32(len(v.Property.Nbrs))
			v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{Height: InitialHeight, Pos: -1, Didx: e.Didx})
			v.Property.NbrMap[e.Didx] = pos
			v.Property.UnknownPosCount++
		}

		v.Property.Nbrs[pos].ResCapOut += int32(e.Property.Weight)
	}
	for i, nbr := range v.Property.Nbrs {
		vtm, tidx := g.NodeVertexMessages(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{Height: v.Property.Height, ResCapOffset: nbr.ResCapOut, Flow: int32(i), SrcId: myId, SrcPos: nbr.Pos, Handshake: true},
		}, vtm, tidx))
	}

	source := VertexCountHelper.NewVertex()
	if source != EmptyValue {
		vtm, tidx := g.NodeVertexMessages(source)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: source,
			Note:   Note{NewMaxVertexCount: true},
		}, vtm, tidx))
	}
	return
}

func (pr *PushRelabel) processMessage(g *Graph, v *Vertex, n graph.Notification[Note]) (sent uint64) {
	// newMaxVertexCount?
	if n.Note.NewMaxVertexCount {
		Assert(v.Property.Type == Source, "Non-source received NewMaxVertexCount")
		v.Property.Height = VertexCountHelper.GetMaxVertexCount()
		v.Property.HeightChanged = true
		return
	}
	alreadySentHeight := false
	// Handle handshakes
	var nbr *Neighbour
	if n.Note.Handshake {
		Assert(n.Note.Flow >= 0, "")
		myPos := n.Note.Flow
		n.Note.Flow = 0
		if n.Note.SrcPos == -1 { // they don't know their position in me
			// Check if they are a new neighbour
			pos, exist := v.Property.NbrMap[n.Note.SrcId]
			if !exist {
				Assert(n.Note.SrcPos == -1, "")
				pos = int32(len(v.Property.Nbrs))
				v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{Height: n.Note.Height, Pos: myPos, Didx: n.Note.SrcId})
				v.Property.NbrMap[n.Note.SrcId] = pos
				// send back their pos and my height
				vtm, tidx := g.NodeVertexMessages(n.Note.SrcId)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: n.Note.SrcId,
					Note:   Note{Height: v.Property.Height, Flow: pos, SrcId: n.Target, SrcPos: myPos, Handshake: true},
				}, vtm, tidx))
				alreadySentHeight = true
			}
			n.Note.SrcPos = pos
		}
		nbr = &v.Property.Nbrs[n.Note.SrcPos]
		if nbr.Pos == -1 {
			nbr.Pos = myPos
			v.Property.UnknownPosCount--
		}
	} else {
		Assert(n.Note.SrcPos >= 0, "")
		nbr = &v.Property.Nbrs[n.Note.SrcPos]
	}
	//Assert(n.Note.SrcId == nbr.Didx, "")

	// Update height and ResCap
	oldResCapIn := nbr.ResCapIn
	nbr.Height = n.Note.Height
	nbr.ResCapIn += n.Note.ResCapOffset

	// handleFlow
	if n.Note.Flow < 0 {
		// retract request
		amount := utils.Max(n.Note.Flow, -nbr.ResCapOut)
		if amount < 0 {
			nbr.ResCapOut -= -amount
			nbr.ResCapIn += -amount
			v.Property.Excess -= -amount

			vtm, tidx := g.NodeVertexMessages(n.Note.SrcId)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
				Target: n.Note.SrcId,
				Note:   Note{Height: v.Property.Height, Flow: -amount, SrcId: n.Target, SrcPos: nbr.Pos},
			}, vtm, tidx))
			alreadySentHeight = true
		}
	} else if n.Note.Flow > 0 {
		// additional flow
		nbr.ResCapOut += n.Note.Flow
		nbr.ResCapIn -= n.Note.Flow
		v.Property.Excess += n.Note.Flow
	}

	// restoreHeightInvariant
	if nbr.ResCapOut > 0 && v.Property.Height > nbr.Height+1 {
		if v.Property.Excess > 0 {
			amount := utils.Min(v.Property.Excess, nbr.ResCapOut)
			v.Property.Excess -= amount
			nbr.ResCapOut -= amount
			nbr.ResCapIn += amount

			vtm, tidx := g.NodeVertexMessages(n.Note.SrcId)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
				Target: n.Note.SrcId,
				Note:   Note{Height: v.Property.Height, Flow: amount, SrcId: n.Target, SrcPos: nbr.Pos},
			}, vtm, tidx))
			alreadySentHeight = true
		}
		if nbr.ResCapOut > 0 {
			Assert(v.Property.Type != Source, "")
			v.Property.Height = nbr.Height + 1
			v.Property.HeightChanged = true
		}
	}

	if !alreadySentHeight && !v.Property.HeightChanged && oldResCapIn <= 0 && nbr.ResCapIn > 0 {
		vtm, tidx := g.NodeVertexMessages(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{Height: v.Property.Height, SrcId: n.Target, SrcPos: nbr.Pos},
		}, vtm, tidx))
	}
	return
}

func (pr *PushRelabel) finalizeVertexState(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// discharge
	if v.Property.Excess > 0 {
		if v.Property.Type == Normal {
			for v.Property.Excess > 0 {
				dischargeSent, nextHeight, nextPush := pr.dischargeOnce(g, v, myId)
				sent += dischargeSent
				if v.Property.Excess == 0 {
					break
				}

				// lift
				Assert(nextHeight != MaxHeight, "")
				v.Property.Height = nextHeight
				v.Property.HeightChanged = true

				// push
				nbr := &v.Property.Nbrs[nextPush]
				amount := utils.Min(v.Property.Excess, nbr.ResCapOut)
				v.Property.Excess -= amount
				nbr.ResCapOut -= amount
				nbr.ResCapIn += amount
				Assert(amount > 0, "")
				vtm, tidx := g.NodeVertexMessages(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Height: v.Property.Height, Flow: amount, SrcId: myId, SrcPos: nbr.Pos},
				}, vtm, tidx))
			}
		} else {
			// Cannot lift
			dischargeSent, _, _ := pr.dischargeOnce(g, v, myId)
			sent += dischargeSent
		}
	} else if v.Property.Excess < 0 && v.Property.Type == Normal && v.Property.Height > 0 {
		v.Property.Height = -VertexCountHelper.GetMaxVertexCount()
		v.Property.HeightChanged = true
	}

	// broadcast heights if needed
	if v.Property.HeightChanged {
		v.Property.HeightChanged = false
		for _, nbr := range v.Property.Nbrs {
			if nbr.ResCapIn > 0 {
				vtm, tidx := g.NodeVertexMessages(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Height: v.Property.Height, SrcId: myId, SrcPos: nbr.Pos},
				}, vtm, tidx))
			}
		}
	}
	return
}

func (pr *PushRelabel) OnUpdateVertex(g *Graph, v *Vertex, n graph.Notification[Note], _ Message) (sent uint64) {
	if v.Property.UnknownPosCount == math.MaxUint32 {
		v.Property.UnknownPosCount = 0
		sent += pr.Init(g, v, n.Target)
		if n.Note != (Note{}) {
			sent += pr.processMessage(g, v, n)
		}
	} else {
		sent += pr.processMessage(g, v, n)
	}

	// Skip the rest if there are more incoming messages
	if n.Activity == 0 && v.Property.UnknownPosCount == 0 {
		sent += pr.finalizeVertexState(g, v, n.Target)
	}

	return
}

func (pr *PushRelabel) dischargeOnce(g *Graph, v *Vertex, myId uint32) (sent uint64, nextHeight int64, nextPush int32) {
	nextHeight = int64(MaxHeight)
	nextPush = -1
	for i := range v.Property.Nbrs {
		nbr := &v.Property.Nbrs[i]
		if nbr.ResCapOut > 0 {
			if !(v.Property.Height > nbr.Height) {
				if nbr.Height+1 < nextHeight {
					nextHeight = nbr.Height + 1
					nextPush = int32(i)
				}
			} else {
				amount := utils.Min(v.Property.Excess, nbr.ResCapOut)
				v.Property.Excess -= amount
				nbr.ResCapOut -= amount
				nbr.ResCapIn += amount
				Assert(amount > 0, "")

				vtm, tidx := g.NodeVertexMessages(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Height: v.Property.Height, Flow: amount, SrcId: myId, SrcPos: nbr.Pos},
				}, vtm, tidx))

				if v.Property.Excess == 0 {
					return
				}
			}
		}
	}
	return sent, nextHeight, nextPush
}

func (pr *PushRelabel) OnEdgeAdd(g *Graph, src *Vertex, sidx uint32, eidxStart int, m Message) (sent uint64) {
	if src.Property.UnknownPosCount == math.MaxUint32 {
		src.Property.UnknownPosCount = 0
		return pr.Init(g, src, sidx)
	} else {
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			e := &src.OutEdges[eidx]
			if e.Didx == sidx || e.Property.Weight <= 0 { // TODO: also skip if the target is source
				continue
			}
			if src.Property.Type == Source {
				src.Property.Excess += int32(e.Property.Weight)
			}

			note := Note{Height: src.Property.Height, ResCapOffset: int32(e.Property.Weight), SrcId: sidx, SrcPos: -1}
			pos, exist := src.Property.NbrMap[e.Didx]
			if !exist {
				pos = int32(len(src.Property.Nbrs))
				src.Property.Nbrs = append(src.Property.Nbrs, Neighbour{Height: InitialHeight, Pos: -1, Didx: e.Didx})
				src.Property.NbrMap[e.Didx] = pos
				src.Property.UnknownPosCount++
			} else {
				note.SrcPos = src.Property.Nbrs[pos].Pos
			}
			nbr := &src.Property.Nbrs[pos]
			nbr.ResCapOut += int32(e.Property.Weight)
			if note.SrcPos == -1 {
				note.Flow = pos
				note.Handshake = true
			} else {
				// restoreHeightInvariant
				if nbr.ResCapOut > 0 && src.Property.Height > nbr.Height+1 {
					if src.Property.Excess > 0 {
						amount := utils.Min(src.Property.Excess, nbr.ResCapOut)
						src.Property.Excess -= amount
						nbr.ResCapOut -= amount
						nbr.ResCapIn += amount
						note.Flow = amount
					}
					if nbr.ResCapOut > 0 {
						Assert(src.Property.Type != Source, "")
						src.Property.Height = nbr.Height + 1
						note.Height = src.Property.Height
						src.Property.HeightChanged = true
					}
				}
			}
			vtm, tidx := g.NodeVertexMessages(e.Didx)
			sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[Note]{Target: e.Didx, Note: note}, vtm, tidx))
		}
	}

	if src.Property.UnknownPosCount == 0 {
		vtm, _ := g.NodeVertexMessages(sidx)
		if atomic.LoadInt32(&vtm.Activity) == 0 {
			sent += pr.finalizeVertexState(g, src, sidx)
		}
	}

	return
}

func (*PushRelabel) OnEdgeDel(g *Graph, src *Vertex, sidx uint32, deletedEdges []Edge, m Message) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabel) OnCheckCorrectness(g *Graph) {
	log.Info().Msg("Ensuring the vertex type is correct")
	sourceInternalId, source := g.NodeVertexFromRaw(SourceRawId)
	sinkInternalId, sink := g.NodeVertexFromRaw(SinkRawId)
	Assert(source.Property.Type == Source, "")
	Assert(sink.Property.Type == Sink, "")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		if v.Property.Type != Normal {
			Assert(internalId == sourceInternalId || internalId == sinkInternalId, "")
		}
	})

	log.Info().Msg("Ensuring all messages are processed")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		Assert(v.Property.UnknownPosCount == 0, "")
		Assert(v.Property.HeightChanged == false, "")
	})

	log.Info().Msg("Checking Pos are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		Assert(len(v.Property.Nbrs) == len(v.Property.NbrMap), "")
		for _, nbr := range v.Property.Nbrs {
			Assert(nbr.Pos >= 0, "")
		}
	})
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for i, nbr := range v.Property.Nbrs {
			targetVertex := g.NodeVertex(nbr.Didx)
			realPos := targetVertex.Property.NbrMap[internalId]
			Assert(nbr.Pos == realPos, "")
			Assert(targetVertex.Property.Nbrs[realPos].Didx == internalId, "")
			Assert(targetVertex.Property.Nbrs[realPos].Pos == int32(i), "")
		}
	})

	log.Info().Msg("Checking ResCapIn are correct")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for _, nbr := range v.Property.Nbrs {
			targetVertex := g.NodeVertex(nbr.Didx)
			realResCap := targetVertex.Property.Nbrs[nbr.Pos].ResCapOut
			Assert(nbr.ResCapIn == realResCap, "")
		}
	})

	log.Info().Msg("Checking the heights of the source and the sink")
	vertexCount := g.NodeVertexCount()
	Assert(source.Property.Height >= int64(vertexCount),
		fmt.Sprintf("Source height %d < # of vertices %d", source.Property.Height, vertexCount))
	Assert(sink.Property.Height == 0,
		fmt.Sprintf("Sink height %d != 0", sink.Property.Height))

	// Check Excess & residual capacity
	log.Info().Msg("Checking excess & residual capacity")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		if v.Property.Type == Normal {
			Assert(v.Property.Excess == 0, "")
		}
		for _, nbr := range v.Property.Nbrs {
			Assert(nbr.ResCapOut >= 0, "")
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight == 0 { // TODO: also skip if the target is source
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}

		for _, nbr := range v.Property.Nbrs {
			capacityResidual += int64(nbr.ResCapOut)
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
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		for _, nbr := range v.Property.Nbrs {
			if nbr.ResCapOut > 0 {
				w := g.NodeVertex(nbr.Didx)
				Assert(nbr.Height == w.Property.Height, "")
			}
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		h := v.Property.Height
		for _, nbr := range v.Property.Nbrs {
			if nbr.ResCapOut > 0 {
				Assert(h <= nbr.Height+1, "")
			}
		}
	})

	// TODO: Print # of vertices in flow
}