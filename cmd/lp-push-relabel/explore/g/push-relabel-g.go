package g

import (
	"fmt"
	"sync"
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
	Type      VertexType
	Excess    int32
	Height    int64
	NewHeight int64 // FIXME

	Nbrs            []Neighbour
	NbrMap          map[uint32]int32 // Id -> Pos
	UnknownPosCount uint32           // shouldn't do anything if it's not 0
	HeightCheckList []int32
}

type EdgeProp struct {
	graph.TimestampWeightedEdge
}

type Message struct {
	Init uint32 // TODO: get rid of this, use other ways to detect the first time visiting a vertex

	// TODO: move all these to a new struct
	Height         int64
	Pos            int32
	NbrHeightCache []atomic.Int64
	CacheUpdated   *atomic.Bool
	Lock           *sync.RWMutex
}

type Note struct {
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
	Name = "PushRelabel: MergedArray, Aggregation(Copy), TrackResCapIn, SkipBroadcastWhenResCapInIs0"
)

var (
	EmptyNote = Note{}
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
	g = graph.LaunchGraphExecution[*EdgeProp, VertexProp, EdgeProp, Message, Note](alg, options, nil, nil)
	return alg.GetMaxFlowValue(g), g
}

func (pr *PushRelabel) GetMaxFlowValue(g *Graph) int32 {
	_, sink := g.NodeVertexFromRaw(SinkRawId)
	return sink.Property.Excess
}

func (pr *PushRelabel) InitAllMail(vertex *Vertex, internalId uint32, rawId graph.RawType) Message {
	return Message{Init: 1}
}

func (pr *PushRelabel) BaseVertexMailbox(v *Vertex, internalId uint32, s *graph.VertexStructure) (m Message) {
	v.Property.Height = InitialHeight
	if s.RawId == SourceRawId {
		v.Property.Type = Source
		v.Property.Height = VertexCountHelper.RegisterSource(internalId)
	} else if s.RawId == SinkRawId {
		v.Property.Type = Sink
		v.Property.Height = 0
	}
	v.Property.NewHeight = v.Property.Height
	return Message{CacheUpdated: new(atomic.Bool), Lock: new(sync.RWMutex)}
}

func expand(pos int, existing *Message, height int64) (newInfo bool) {
	existing.Lock.Lock()
	ns := existing.NbrHeightCache
	prevLen := len(ns)
	if prevLen <= pos { // check again
		ns = append(ns, make([]atomic.Int64, pos+1-prevLen)...)
		for i := prevLen; i < len(ns); i++ {
			ns[i].Store(InitialHeight)
		}
		existing.NbrHeightCache = ns
	}
	oldHeight := ns[pos].Swap(height)
	if oldHeight > height {
		newInfo = true
	}
	existing.Lock.Unlock()
	return newInfo
}

func (*PushRelabel) MailMerge(incoming Message, sidx uint32, existing *Message) (newInfo bool) {
	if incoming.Init != 0 {
		atomic.StoreUint32(&existing.Init, 1)
		return true
	}

	existing.Lock.RLock()
	ns := existing.NbrHeightCache
	if len(ns) <= int(incoming.Pos) {
		existing.Lock.RUnlock()
		newInfo = expand(int(incoming.Pos), existing, incoming.Height)
	} else {
		oldHeight := ns[incoming.Pos].Swap(incoming.Height)
		existing.Lock.RUnlock()
		newInfo = oldHeight > incoming.Height
	}
	existing.CacheUpdated.Store(true)
	return newInfo
}

func (*PushRelabel) MailRetrieve(existing *Message, v *Vertex) (outgoing Message) {
	existing.Lock.RLock()
	if atomic.LoadUint32(&existing.Init) != 0 && atomic.SwapUint32(&existing.Init, 0) != 0 {
		outgoing.Init = 1
	}

	syncHeights := existing.CacheUpdated.Swap(false)
	if syncHeights {
		ns := existing.NbrHeightCache
		for i := range ns {
			newHeight := ns[i].Load()
			nbr := &v.Property.Nbrs[i]
			if nbr.Height > newHeight && nbr.ResCapOut > 0 {
				v.Property.HeightCheckList = append(v.Property.HeightCheckList, int32(i))
			}
			nbr.Height = newHeight
		}
	}
	existing.Lock.RUnlock()

	return outgoing
}

func (pr *PushRelabel) Init(g *Graph, v *Vertex, myId uint32) (sent uint64) {
	// Iterate over existing edges
	Assert(len(v.Property.NbrMap) == 0, "")
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
			v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{Height: InitialHeight, Pos: -1, Didx: e.Didx})
			v.Property.NbrMap[e.Didx] = pos
			v.Property.UnknownPosCount++
		}

		v.Property.Nbrs[pos].ResCapOut += int32(e.Property.Weight)
	}
	for i, nbr := range v.Property.Nbrs {
		mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
		sent += g.EnsureSend(g.ActiveNotification(myId, graph.Notification[Note]{
			Target: nbr.Didx,
			Note:   Note{ResCapOffset: nbr.ResCapOut, Flow: int32(i), SrcId: myId, SrcPos: nbr.Pos, Handshake: true},
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

func (pr *PushRelabel) OnUpdateVertex(g *Graph, v *Vertex, n graph.Notification[Note], m Message) (sent uint64) {
	if m.Init != 0 {
		sent += pr.Init(g, v, n.Target)
	}
	// newMaxVertexCount?
	if n.Note.NewMaxVertexCount {
		Assert(v.Property.Type == Source, "Non-source received NewMaxVertexCount")
		v.Property.NewHeight = VertexCountHelper.GetMaxVertexCount()
	} else if n.Note != EmptyNote {
		var alreadySentHeight bool
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
					v.Property.Nbrs = append(v.Property.Nbrs, Neighbour{Height: InitialHeight, Pos: myPos, Didx: n.Note.SrcId})
					v.Property.NbrMap[n.Note.SrcId] = pos
					// send back their pos and my height
					mailbox, tidx := g.NodeVertexMailbox(n.Note.SrcId)
					pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: myPos}, n.Target, &mailbox.Inbox)
					sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
						Target: n.Note.SrcId,
						Note:   Note{Flow: pos, SrcId: n.Target, SrcPos: myPos, Handshake: true},
					}, mailbox, tidx))
					alreadySentHeight = true
				} else {
					// already told them
				}
				nbr = &v.Property.Nbrs[pos]
			} else {
				nbr = &v.Property.Nbrs[n.Note.SrcPos]
			}
			if nbr.Pos == -1 {
				nbr.Pos = myPos
				v.Property.UnknownPosCount--
				if !alreadySentHeight {
					mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
					if pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: myPos}, n.Target, &mailbox.Inbox) {
						sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: nbr.Didx}, mailbox, tidx))
					}
					alreadySentHeight = true
				}
			}
		} else {
			Assert(n.Note.SrcPos >= 0, "")
			nbr = &v.Property.Nbrs[n.Note.SrcPos]
		}
		//Assert(n.Note.SrcId == nbr.Didx, "")

		// Update ResCap
		oldResCapIn := nbr.ResCapIn
		if n.Note.ResCapOffset != 0 {
			nbr.ResCapIn += n.Note.ResCapOffset
		}

		// handleFlow
		if n.Note.Flow < 0 {
			// retract request
			amount := utils.Max(n.Note.Flow, -nbr.ResCapOut)
			if amount < 0 {
				nbr.ResCapOut -= -amount
				nbr.ResCapIn += -amount
				v.Property.Excess -= -amount

				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: nbr.Pos}, n.Target, &mailbox.Inbox)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Flow: -amount, SrcId: n.Target, SrcPos: nbr.Pos},
				}, mailbox, tidx))
			}
		} else if n.Note.Flow > 0 {
			// additional flow
			nbr.ResCapOut += n.Note.Flow
			nbr.ResCapIn -= n.Note.Flow
			v.Property.Excess += n.Note.Flow
		}

		// restoreHeightInvariant
		if n.Note.Flow > 0 {
			v.Property.HeightCheckList = append(v.Property.HeightCheckList, n.Note.SrcPos)
		}

		if !alreadySentHeight && oldResCapIn <= 0 && nbr.ResCapIn > 0 {
			mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
			if pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: nbr.Pos}, n.Target, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: nbr.Didx}, mailbox, tidx))
			}
		}
	}

	// done with the message itself

	// Skip the rest if there are more incoming messages
	if n.Activity > 0 || v.Property.UnknownPosCount > 0 {
		return
	}

	// restoreHeightInvariant
	for _, pos := range v.Property.HeightCheckList {
		nbr := &v.Property.Nbrs[pos]
		if nbr.ResCapOut > 0 && nbr.Height+1 < v.Property.NewHeight {
			if v.Property.Excess > 0 {
				amount := utils.Min(v.Property.Excess, nbr.ResCapOut)
				v.Property.Excess -= amount
				nbr.ResCapOut -= amount
				nbr.ResCapIn += amount

				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: nbr.Pos}, n.Target, &mailbox.Inbox)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Flow: amount, SrcId: n.Target, SrcPos: nbr.Pos},
				}, mailbox, tidx))
			}
			if nbr.ResCapOut > 0 {
				Assert(v.Property.Type != Source, "")
				v.Property.NewHeight = nbr.Height + 1
			}
		}
	}
	v.Property.HeightCheckList = v.Property.HeightCheckList[:0]

	// discharge
	if v.Property.Excess > 0 {
		if v.Property.Type == Normal {
			for v.Property.Excess > 0 {
				dischargeSent, nextHeight := pr.dischargeOnce(g, v, n)
				sent += dischargeSent
				if v.Property.Excess == 0 {
					break
				}
				// lift
				Assert(nextHeight != MaxHeight, "")
				v.Property.NewHeight = nextHeight
			}
		} else {
			// Cannot lift
			dischargeSent, _ := pr.dischargeOnce(g, v, n)
			sent += dischargeSent
		}
	} else if v.Property.Excess < 0 && v.Property.Type == Normal && v.Property.Height > 0 {
		v.Property.NewHeight = -VertexCountHelper.GetMaxVertexCount()
	}

	// broadcast heights if needed
	if v.Property.NewHeight != v.Property.Height {
		v.Property.Height = v.Property.NewHeight
		for _, nbr := range v.Property.Nbrs {
			if nbr.ResCapIn > 0 {
				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				if pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: nbr.Pos}, n.Target, &mailbox.Inbox) {
					sent += g.EnsureSend(g.UniqueNotification(n.Target, graph.Notification[Note]{Target: nbr.Didx}, mailbox, tidx))
				}
			}
		}
	}

	return
}

func (pr *PushRelabel) dischargeOnce(g *Graph, v *Vertex, n graph.Notification[Note]) (sent uint64, nextHeight int64) {
	nextHeight = int64(MaxHeight)
	for i := range v.Property.Nbrs {
		nbr := &v.Property.Nbrs[i]
		if nbr.ResCapOut > 0 {
			if !(v.Property.NewHeight > nbr.Height) {
				nextHeight = utils.Min(nextHeight, nbr.Height+1)
			} else {
				amount := utils.Min(v.Property.Excess, nbr.ResCapOut)
				v.Property.Excess -= amount
				nbr.ResCapOut -= amount
				nbr.ResCapIn += amount
				Assert(amount > 0, "")

				mailbox, tidx := g.NodeVertexMailbox(nbr.Didx)
				pr.MailMerge(Message{Height: v.Property.NewHeight, Pos: nbr.Pos}, n.Target, &mailbox.Inbox)
				sent += g.EnsureSend(g.ActiveNotification(n.Target, graph.Notification[Note]{
					Target: nbr.Didx,
					Note:   Note{Flow: amount, SrcId: n.Target, SrcPos: nbr.Pos},
				}, mailbox, tidx))

				if v.Property.Excess == 0 {
					return
				}
			}
		}
	}
	return sent, nextHeight
}

func (pr *PushRelabel) OnEdgeAdd(g *Graph, src *Vertex, sidx uint32, eidxStart int, m Message) (sent uint64) {
	// TODO
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
		mailbox, _ := g.NodeVertexMailbox(internalId)
		Assert(mailbox.Inbox.Init == 0, "")
		Assert(mailbox.Inbox.Height == 0, "")
		Assert(mailbox.Inbox.Pos == 0, "")
		Assert(v.Property.UnknownPosCount == 0, "")
		Assert(v.Property.Height == v.Property.NewHeight, "")
		Assert(len(v.Property.HeightCheckList) == 0, "")
		Assert(len(mailbox.Inbox.NbrHeightCache) == len(v.Property.Nbrs), "")
		for i := range mailbox.Inbox.NbrHeightCache {
			height := mailbox.Inbox.NbrHeightCache[i].Load()
			Assert(height >= v.Property.Nbrs[i].Height, "")
		}
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
			if e.Didx == internalId || e.Property.Weight <= 0 || e.Didx == VertexCountHelper.GetSourceId() {
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
				Assert(nbr.Height <= g.NodeVertex(nbr.Didx).Property.Height, "")
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
