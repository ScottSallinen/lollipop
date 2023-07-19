package main

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

type Colouring struct{}

// A strategy (for static graphs) is to use wait count to have each vertex pick a colour "in order".
// Note that the Base mail for a dynamic graph would have no beginning edges, so wait count would be zero -- thus this strategy is only useful for static graphs.
const USE_WAIT_COUNT = false

const EMPTY_VAL = (math.MaxUint32) >> 1
const MSB_MASK = (1 << 31) - 1
const MSB = (1 << 31)

type VertexProperty struct {
	Colour         uint32
	coloursIndexed utils.Bitmap // Mark colours that are already used by neighbours
}

type EdgeProperty struct {
	graph.TimestampEdge
}

type Mail struct {
	NbrScratch []uint32 // Colours of neighbours
	Pos        uint32   // Unique identifier of the mail sender, that the mail receiver can use to (re)identify the sender. The value is ordinal, and corresponds uniquely to an edge (that has ever existed) that targets the receiver.
	Colour     uint32   // Multi-purposed (fake union); used as Wait count on existing (if USE_WAIT_COUNT is true)
	Mutex      *sync.RWMutex
}

type Note struct{}

func (VertexProperty) New() (vp VertexProperty) {
	vp.Colour = EMPTY_VAL
	vp.coloursIndexed.Grow(127) // Two of 8 bytes is a good start.
	return vp
}

func (Mail) New() (m Mail) {
	m.Pos = EMPTY_VAL
	m.Colour = EMPTY_VAL
	return m
}

// Dummy hash function (just remove tidx). Should do something else for different id types.
func hash(id uint32) (hash uint32) {
	return (id & graph.THREAD_MASK)
}

// Returns true if p1 has priority over p2.
// Smallest ID first is better for dynamic (vertex ID increments)
func comparePriority(p1, p2 uint32, id1, id2 uint32) bool {
	return p1 < p2 || (p1 == p2 && id1 < id2)
}

func (*Colouring) BaseVertexMailbox(vertex *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, s *graph.VertexStructure) (m Mail) {
	m.Mutex = new(sync.RWMutex)
	m.Pos = internalId // This is the vertex ID, set for the base mailbox of the vertex.
	m.Colour = 0       // Used as wait count (if enabled) for the base mailbox of the vertex.

	if USE_WAIT_COUNT { // Initialize WaitCount (only relevant for static graphs)
		myPriority := hash(internalId)
		for i := 0; i < len(vertex.OutEdges); i++ {
			didx := vertex.OutEdges[i].Didx
			if comparePriority(hash(didx), myPriority, didx, internalId) {
				m.Colour++ // If the edge has priority, we need to wait for it.
			}
		}
	}

	edgeAmount := int(s.InEventPos)           // Note: this will be zero for dynamic graphs. For static, this should be equal to len(src.InEdges), as the graph is undirected.
	m.NbrScratch = make([]uint32, edgeAmount) // This is for a thread unsafe view // m.NbrScratch.Store(ns)

	for i := 0; i < edgeAmount; i++ { // Note: does nothing for dynamic graphs.
		m.NbrScratch[i] = EMPTY_VAL
	}
	return m
}

// Self mail (init). Needed, but the mail value itself is irrelevant -- it is uniquely identified as mail from itself to itself.
func (*Colouring) InitAllMail(vertex *graph.Vertex[VertexProperty, EdgeProperty], internalId uint32, rawId graph.RawType) (m Mail) {
	return m
}

// Dynamic: need to reallocate. Expand the size of existing.NbrScratch. New size must fit NbrScratch[ln], so we expect after that len(NbrScratch) == ln + 1.
func mExpand(ln int, existing *Mail, colour uint32) {
	existing.Mutex.Lock()
	ns := existing.NbrScratch
	prevLen := len(ns)
	if prevLen <= ln { // check again
		ns = append(ns, make([]uint32, (ln+1-prevLen))...)
		for i := prevLen; i < len(ns); i++ {
			ns[i] = EMPTY_VAL
		}
		existing.NbrScratch = ns
	}
	ns[ln] = colour
	existing.Mutex.Unlock()
}

func (*Colouring) MailMerge(incoming Mail, sidx uint32, existing *Mail) (newInfo bool) {
	didx := existing.Pos
	if sidx == didx { // Self mail (init)
		if !USE_WAIT_COUNT {
			return true // Not using wait count, always on self mail (at-least-once)
		}
		return (atomic.LoadUint32(&existing.Colour) == 0) // Might cause a second update cycle (but it does not matter)
	}

	targetPriority := comparePriority(hash(didx), hash(sidx), didx, sidx)
	colour := incoming.Colour
	if !targetPriority {
		colour = colour | MSB // If we have priority, set MSB
	}

	existing.Mutex.RLock()
	ns := existing.NbrScratch
	if len(ns) <= int(incoming.Pos) {
		existing.Mutex.RUnlock()
		mExpand(int(incoming.Pos), existing, colour)
	} else {
		atomic.StoreUint32(&ns[incoming.Pos], colour)
		existing.Mutex.RUnlock()
	}

	if targetPriority {
		return false // If target (existing) has priority, no need to notify it
	}

	if !USE_WAIT_COUNT {
		return true // Not using wait count, need to ensure update when a priority neighbour tells us their colour
	}

	// Check wait count.
	if atomic.LoadUint32(&existing.Colour) > 0 {
		newWaitCount := atomic.AddUint32(&existing.Colour, ^uint32(0)) // Subtract 1
		if newWaitCount == 0 {
			atomic.StoreUint32(&existing.Colour, ^uint32(0)) // Likely to prevent second update cycle
			return true
		}
	}
	return false
}

func (*Colouring) MailRetrieve(existing *Mail, vertex *graph.Vertex[VertexProperty, EdgeProperty]) (outgoing Mail) {
	prop := &vertex.Property
	ourColour := prop.Colour
	prop.coloursIndexed.Zeroes()

	existing.Mutex.RLock()
	ns := existing.NbrScratch
	for i := range ns {
		nsc := atomic.LoadUint32(&ns[i])
		col := (nsc & MSB_MASK)
		if col <= uint32(len(ns)) { // Ignore colour > len(ns), since this implies we will have a colour available within |InDegree|
			if !prop.coloursIndexed.QuickSet(col) {
				prop.coloursIndexed.Set(col)
			}
			// If MSB is set, the neighbour had declared priority (calculated in merge function).
			if ((nsc & MSB) != 0) && (col == ourColour) {
				prop.Colour = EMPTY_VAL
			}
		}
	}
	existing.Mutex.RUnlock()

	return outgoing
}

// Note the mail from MailRetrieve isn't used; the MailRetrieve function itself applies the values into the vertex already -- in this case, updating the neighbour colour indexes.
func (alg *Colouring) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], notif graph.Notification[Note], _ Mail) (sent uint64) {
	best := src.Property.coloursIndexed.FirstUnused()

	if src.Property.Colour == best {
		return 0 // If no change, nothing to do.
	}
	src.Property.Colour = best

	// Tell our new colour to all neighbours.
	for _, e := range src.OutEdges {
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		if alg.MailMerge(Mail{Colour: src.Property.Colour, Pos: e.Pos}, notif.Target, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(notif.Target, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
		}
	}
	return sent
}

// OnEdgeAdd: Function called upon a new edge add (which also bundles a visit, including any new Data).
// The view here is **post** addition (the edges are already appended to the edge list)
// Note: eidxStart is the first position of new edges in the OutEdges array. (Edges may contain multiple edges with the same destination)
func (alg *Colouring) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, eidxStart int, m Mail) (sent uint64) {
	// Update first. If we already targeted all neighbours, we can skip the rest.
	if sent = alg.OnUpdateVertex(g, src, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
		return sent
	}
	// Target all new edges
	srcPriority := hash(sidx)
	for eidx := eidxStart; eidx < len(src.OutEdges); eidx++ {
		didx := src.OutEdges[eidx].Didx
		// If we have priority, tell the other vertex our colour.
		// Since we are always undirected, the other vertex will perform the opposite to us (priority-wise.)
		if comparePriority(srcPriority, hash(didx), sidx, didx) {
			mailbox, tidx := g.NodeVertexMailbox(didx)
			if alg.MailMerge(Mail{Colour: src.Property.Colour, Pos: src.OutEdges[eidx].Pos}, sidx, &mailbox.Inbox) {
				sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: didx}, mailbox, tidx))
			}
		}
	}
	return sent
}

// This function is to be called with a set of edge deletion events.
func (alg *Colouring) OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], sidx uint32, deletedEdges []graph.Edge[EdgeProperty], m Mail) (sent uint64) {
	// Update first.
	sent += alg.OnUpdateVertex(g, src, graph.Notification[Note]{Target: sidx}, m)

	for _, e := range deletedEdges {
		// Just notify deleted edge; they will set our pos to EMPTY_VAL so they no longer will care about us.
		// We do not try to greedily re-colour here, as the undirected counterpart will notify us of their deletion, causing us to update.
		mailbox, tidx := g.NodeVertexMailbox(e.Didx)
		if alg.MailMerge(Mail{Colour: EMPTY_VAL, Pos: e.Pos}, sidx, &mailbox.Inbox) {
			sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
		}
	}
	return sent
}
