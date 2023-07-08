package k

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
	Height int64
	ResCap int32
	Pos    int32 // Position of me in the neighbour, -1 means unknown
	Didx   uint32
}

type VertexProp struct {
	Type          VertexType
	Excess        int32
	Height        int64
	HeightChanged bool

	Nbrs            []Neighbour
	NbrMap          map[uint32]int32 // Id -> Pos
	UnknownPosCount uint32           // shouldn't do anything if it's not 0
	HeightEpochNum  uint32
}

type EdgeProp struct {
	graph.TimestampWeightedEdge
}

type Mail struct{}

type Note struct {
	Height         int64
	HeightEpochNum uint32
	Flow           int32 // Could be used for my position when hand-shaking

	SrcId  uint32 // FIXME: better if this is given by the framework
	SrcPos int32  // position of the sender in the receiver's InNbrs or OutEdges, -1 means unknown

	Handshake         bool
	NewMaxVertexCount bool // source needs to increase height, should not interpret other fields if this is set to true
	NewHeightEpoch    bool
}

type Graph = graph.Graph[VertexProp, EdgeProp, Mail, Note]
type Vertex = graph.Vertex[VertexProp, EdgeProp]
type Edge = graph.Edge[EdgeProp]

const (
	Name = "PushRelabel (K)"
)

func (VertexProp) New() (new VertexProp) {
	new.NbrMap = make(map[uint32]int32)
	return new
}

func (Mail) New() (new Mail) {
	return new
}

func Run(options graph.GraphOptions) (maxFlow int32, g *Graph) {
	alg := new(PushRelabel)
	GlobalRelabelingHelper.Reset()
	TimeSeriesReset()
	g = graph.LaunchGraphExecution[*EdgeProp, VertexProp, EdgeProp, Mail, Note](alg, options)
	return alg.GetMaxFlowValue(g), g
}

func (pr *PushRelabel) GetMaxFlowValue(g *Graph) int32 {
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabel) InitAllMail(_ *Vertex, _ uint32, _ graph.RawType) Mail {
	return Mail{}
}

func (pr *PushRelabel) BaseVertexMailbox(v *Vertex, internalId uint32, rawId graph.RawType) (m Mail) {
	v.Property.Height = InitialHeight
	if rawId == SourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
		GlobalRelabelingHelper.RegisterSource(internalId)
	} else if rawId == SinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
		GlobalRelabelingHelper.RegisterSink(internalId)
	}
	v.Property.UnknownPosCount = math.MaxUint32 // Make as uninitialized
	return m
}

func (*PushRelabel) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	return true
}

func (*PushRelabel) MailRetrieve(existing *Mail, vertex *Vertex) (outgoing Mail) {
	return outgoing
}

func (pr *PushRelabel) Init(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// Iterate over existing edges
	for eidx := range v.OutEdges {
		e := &v.OutEdges[eidx]
		if e.Didx == myId || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
			continue
		}
		if v.Property.Type == Source {
			v.Property.Excess += int32(e.Property.Weight)
		}

		pos, exist := v.Property.NbrMap[e.Didx]
		if !exist {
			pos = int32(len(v.Property.Nbrs))
			v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{Height: InitialHeight, ResCap: 0, Pos: -1, Didx: e.Didx})
			v.Property.NbrMap[e.Didx] = pos
			v.Property.UnknownPosCount++
		}

		v.Property.Nbrs[pos].ResCap += int32(e.Property.Weight)
	}
	for i, nbr := range v.Property.Nbrs {
		mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, Flow: int32(i), SrcId: myId, SrcPos: nbr.Pos, Handshake: true},
		}, mailbox, tidx))
	}

	source := VertexCountHelper.NewVertex()
	if source != EmptyValue {
		mailbox, tidx := g.NodeVertexMailbox(source)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: source,
			Note:   Note{NewMaxVertexCount: true},
		}, mailbox, tidx))
	}
	return
}

func (pr *PushRelabel) OnUpdateVertex(g *Graph, v *Vertex, n graph.Notification[Note], m Mail) (sent uint64) {
	if v.Property.UnknownPosCount == math.MaxUint32 {
		v.Property.UnknownPosCount = 0
		sent += pr.Init(g, v, n.Target)
		if n.Note != (Note{}) { // FIXME: could be a real useful message
			sent += pr.processMessage(g, v, n)
		}
	} else {
		sent += pr.processMessage(g, v, n)
	}

	if n.Activity == 0 && v.Property.UnknownPosCount == 0 { // Skip if there are more incoming messages
		sent += pr.finalizeVertexState(g, v, n.Target)
	}
	return
}

func (pr *PushRelabel) dischargeOnce(g *Graph, v *Vertex, myId uint32) (sent uint64, nextHeight int64, nextPush int32) {
	nextHeight = int64(MaxHeight)
	nextPush = -1
	for i := range v.Property.Nbrs {
		nbr := &v.Property.Nbrs[i]
		if nbr.ResCap > 0 {
			if !(v.Property.Height > nbr.Height) {
				if nbr.Height+1 < nextHeight {
					nextHeight = nbr.Height + 1
					nextPush = int32(i)
				}
			} else {
				amount := utils.Min(v.Property.Excess, nbr.ResCap)
				v.Property.Excess -= amount
				nbr.ResCap -= amount
				Assert(amount > 0, "")

				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, Flow: amount, SrcId: myId, SrcPos: nbr.Pos},
				}, mailbox, tidx))

				if v.Property.Excess == 0 {
					return
				}
			}
		}
	}
	return sent, nextHeight, nextPush
}

func (pr *PushRelabel) processMessage(g *Graph, v *Vertex, n graph.Notification[Note]) (sent uint64) {
	// Special message?
	if n.Note.NewMaxVertexCount {
		Assert(v.Property.Type == Source, "Non-source received NewMaxVertexCount")
		v.Property.Height = VertexCountHelper.GetMaxVertexCount()
		v.Property.HeightChanged = true
		return
	} else if n.Note.NewHeightEpoch {
		Assert(v.Property.Type != Normal, "")
		if v.Property.Type == Source {
			v.Property.Height = VertexCountHelper.GetMaxVertexCount()
		}
		v.Property.HeightEpochNum += 1
		v.Property.HeightChanged = true
		return
	}

	// Check HeightEpoch (for global relabeling)
	if GlobalRelabelingEnabled {
		if n.Note.HeightEpochNum < v.Property.HeightEpochNum {
			n.Note.Height = MaxHeight
		} else if n.Note.HeightEpochNum > v.Property.HeightEpochNum {
			v.Property.HeightEpochNum = n.Note.HeightEpochNum
			v.Property.Height = MaxHeight
			v.Property.HeightChanged = true
			for i := range v.Property.Nbrs {
				v.Property.Nbrs[i].Height = MaxHeight
			}
		}
	}

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
				nbr = &v.Property.Nbrs[pos]
				// send back their pos and my height
				mailbox, tidx := g.NodeVertexMailbox(n.Note.SrcId)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: n.Note.SrcId,
					Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, Flow: pos, SrcId: n.Target, SrcPos: myPos, Handshake: true},
				}, mailbox, tidx))
			} else {
				// already told them
				nbr = &v.Property.Nbrs[pos]
			}
		} else {
			nbr = &v.Property.Nbrs[n.Note.SrcPos]
		}
		if nbr.Pos == -1 {
			nbr.Pos = myPos
			v.Property.UnknownPosCount--
		}
	} else {
		nbr = &v.Property.Nbrs[n.Note.SrcPos]
	}
	Assert(n.Note.SrcId == nbr.Didx, "")

	// Update height
	nbr.Height = n.Note.Height

	// handleFlow
	if n.Note.Flow < 0 {
		// retract request
		amount := utils.Max(n.Note.Flow, -nbr.ResCap)
		if amount < 0 {
			nbr.ResCap -= -amount
			v.Property.Excess -= -amount

			mailbox, tidx := g.NodeVertexMailbox(n.Note.SrcId)
			sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
				Target: n.Note.SrcId,
				Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, Flow: -amount, SrcId: n.Target, SrcPos: nbr.Pos},
			}, mailbox, tidx))
		}
	} else if n.Note.Flow > 0 {
		// additional flow
		nbr.ResCap += n.Note.Flow
		v.Property.Excess += n.Note.Flow
	}

	sent += pr.restoreHeightInvariant(g, v, nbr, n.Target)

	return
}

func (pr *PushRelabel) restoreHeightInvariant(g *Graph, v *Vertex, nbr *Neighbour, myId uint32) (sent uint64) {
	if nbr.ResCap > 0 && v.Property.Height > nbr.Height+1 {
		if v.Property.Excess > 0 {
			amount := utils.Min(v.Property.Excess, nbr.ResCap)
			v.Property.Excess -= amount
			nbr.ResCap -= amount

			mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
			sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, Flow: amount, SrcId: myId, SrcPos: nbr.Pos},
			}, mailbox, tidx))
		}
		if nbr.ResCap > 0 {
			Assert(v.Property.Type != Source, "")
			v.Property.Height = nbr.Height + 1
			v.Property.HeightChanged = true
		}
	}
	return sent
}

func (pr *PushRelabel) finalizeVertexState(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// discharge
	if v.Property.Excess > 0 {
		if v.Property.Height < MaxHeight {
			if v.Property.Type == Normal {
				for v.Property.Excess > 0 {
					dischargeSent, nextHeight, nextPush := pr.dischargeOnce(g, v, myId)
					sent += dischargeSent
					if v.Property.Excess == 0 {
						break
					}
					// lift
					if nextHeight >= MaxHeight {
						break
					}
					v.Property.Height = nextHeight
					v.Property.HeightChanged = true
					if GlobalRelabelingEnabled {
						sent += GlobalRelabelingHelper.OnLift(func(sinkId uint32) uint64 {
							mailbox, tidx := g.NodeVertexMailbox(sinkId)
							return g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
								Target: sinkId,
								Note:   Note{NewHeightEpoch: true},
							}, mailbox, tidx))
						})
					}

					// push
					nbr := &v.Property.Nbrs[nextPush]
					amount := utils.Min(v.Property.Excess, nbr.ResCap)
					v.Property.Excess -= amount
					nbr.ResCap -= amount
					Assert(amount > 0, "")
					mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
					sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
						Target: nbr.Didx,
						Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, Flow: amount, SrcId: myId, SrcPos: nbr.Pos},
					}, mailbox, tidx))
				}
			} else {
				// Cannot lift
				dischargeSent, _, _ := pr.dischargeOnce(g, v, myId)
				sent += dischargeSent
			}
		}
	} else if v.Property.Excess < 0 && v.Property.Type == Normal && v.Property.Height > 0 {
		v.Property.Height = -VertexCountHelper.GetMaxVertexCount()
		v.Property.HeightChanged = true
	}

	// broadcast heights if needed
	if v.Property.HeightChanged {
		v.Property.HeightChanged = false
		for _, nbr := range v.Property.Nbrs {
			mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
			sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
				Target: nbr.Didx,
				Note:   Note{Height: v.Property.Height, HeightEpochNum: v.Property.HeightEpochNum, SrcId: myId, SrcPos: nbr.Pos},
			}, mailbox, tidx))
		}
	}
	return sent
}

func (pr *PushRelabel) OnEdgeAdd(g *Graph, src *Vertex, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	if src.Property.UnknownPosCount == math.MaxUint32 {
		src.Property.UnknownPosCount = 0
		sent += pr.Init(g, src, sidx)
	} else {
		for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
			e := &src.OutEdges[eidx]
			if e.Didx == sidx || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
				continue
			}
			if src.Property.Type == Source {
				src.Property.Excess += int32(e.Property.Weight)
			}

			pos, exist := src.Property.NbrMap[e.Didx]
			if !exist {
				pos = int32(len(src.Property.Nbrs))
				src.Property.Nbrs = append(src.Property.Nbrs, Neighbour{Height: InitialHeight, Pos: -1, Didx: e.Didx})
				src.Property.NbrMap[e.Didx] = pos
				src.Property.UnknownPosCount++
			}

			nbr := &src.Property.Nbrs[pos]
			nbr.ResCap += int32(e.Property.Weight)
			if nbr.Pos == -1 {
				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				sent += g.EnsureSend(g.ActiveNotification(sidx, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Height: src.Property.Height, HeightEpochNum: src.Property.HeightEpochNum, Flow: pos, SrcId: sidx, SrcPos: nbr.Pos, Handshake: true},
				}, mailbox, tidx))
			} else {
				sent += pr.restoreHeightInvariant(g, src, nbr, sidx)
			}
		}
	}
	mailbox, _ := g.NodeVertexMailbox(sidx)
	if src.Property.UnknownPosCount == 0 && atomic.LoadInt32(&mailbox.Activity) == 0 {
		sent += pr.finalizeVertexState(g, src, sidx)
	}
	return
}

func (*PushRelabel) OnEdgeDel(g *Graph, src *Vertex, sidx uint32, deletedEdges []Edge, m Mail) (sent uint64) {
	// TODO
	return 0
}

func (*PushRelabel) OnCheckCorrectness(g *Graph) {
	sourceInternalId, source := g.NodeVertexFromRaw(SourceRawId)
	sinkInternalId, sink := g.NodeVertexFromRaw(SinkRawId)
	if source == nil || sink == nil {
		log.Debug().Msg("Skipping OnCheckCorrectness due to missing source or sink")
		return
	}

	log.Info().Msg("Ensuring the vertex type is correct")
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
			Assert(nbr.ResCap >= 0, "")
		}
	})

	log.Info().Msg("Checking sum of edge capacities in the original graph == in the residual graph")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		capacityOriginal := int64(0)
		capacityResidual := int64(0)
		for _, e := range v.OutEdges {
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
				continue
			}
			capacityOriginal += int64(e.Property.Weight)
		}

		for _, nbr := range v.Property.Nbrs {
			capacityResidual += int64(nbr.ResCap)
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
			w := g.NodeVertex(nbr.Didx)
			Assert(nbr.Height == w.Property.Height, "")
		}
	})

	log.Info().Msg("Checking height invariants")
	g.NodeForEachVertex(func(ordinal, internalId uint32, v *Vertex) {
		h := v.Property.Height
		for _, nbr := range v.Property.Nbrs {
			if nbr.ResCap > 0 {
				Assert(h <= nbr.Height+1, "")
			}
		}
	})

	// TODO: Print # of vertices in flow
}
