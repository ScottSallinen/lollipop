package main

import (
	"math"
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

// Temporal Pathing algorithm. VERY UNOPTIMIZED! THIS IS JUST A PRELIMINARY PROOF OF CONCEPT!
// There are many possible different variants that could be imagined when defining "temporal shortest paths".
// For this algorithmic variant, I have focused on shortest path through "time" (quickest timestamp arrival), and have mostly discounted "space" (hops across vertices).
// Also, I have mostly focused on the "extend" variant, which sets the reachability end time of an edge to become the end time of that edge (rather than the incoming window end time).
// I can imagine other variants that:
//   - Track and consider spatial distances (i.e., number of hops) as part of the overall "cost".
//   - Extend not necessarily until the edge end, but perhaps for some defined duration.
//   - Consider a traversal "cost" (in units of time) to traverse an edge (perhaps this defined by an edge weight).
//   - Allow a path to continue across an edge that existed before the arrival at a vertex, so long as that path isn't yet deleted (i.e., the path represents an ongoing relationship, rather than an instantaneous causal impulse at edge creation)
//   - And many more variants, I'm sure...
type TP struct{}

const EXTEND_TO_EDGE_END = true // For the "Extend" variant of the algorithm, the so long as an edge is initially reachable, the end time of that reach then becomes the end time of that edge.

const INFINITY = math.MaxUint64

type Path struct {
	Start uint64
	End   uint64
	Hops  uint64
}

type InboundEntry struct {
	Inbound Path // An InboundEntry is just the path for now, though I imagine this could be extended to contain more data.
}

type VertexProperty struct {
	Windows       []Path         // This is the current view of the vertex's reachability windows it has, through time. It is always sorted by start.
	nextWindows   []Path         // Temporary cache/workspace for building the next view of the vertex's reachability windows.
	priorityQueue utils.PQ[Path] // Temporary priority queue of paths, it's also a cache (so we don't realloc the memory every time).
	changed       bool           // Temporary flag to quickly denote if there was a change in the windows (aka, if the vertex has information to propagate to its neighbours).
}

func (VertexProperty) New() (p VertexProperty) {
	return p
}

type EdgeProperty struct {
	graph.TimeRangeEdge
	graph.NoRaw
}

type Mail struct {
	NewPath    Path
	NbrScratch []InboundEntry
	Mutex      *sync.RWMutex
	Pos        uint32
}

func (Mail) New() (m Mail) {
	return m // Overridden by BaseVertexMailbox.
}

type Note struct{} // Unused

func (p Path) Less(other Path) bool {
	return p.Start < other.Start // We want Pop to give us the smallest
}

func (p Path) String() string {
	if p.End == INFINITY {
		return "{" + utils.F("%3d", p.Hops) + "}[" + utils.V(p.Start) + ", ...)"
	} else {
		return "{" + utils.F("%3d", p.Hops) + "}[" + utils.V(p.Start) + ", " + utils.V(p.End) + ")"
	}
}

func (*TP) BaseVertexMailbox(vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, internalId uint32, s *graph.VertexStructure) (m Mail) {
	m.NbrScratch = make([]InboundEntry, s.InEventPos) // For static graphs, this is the number of incoming edges.
	for i := range m.NbrScratch {
		m.NbrScratch[i].Inbound = Path{INFINITY, 0, INFINITY}
	}
	m.Pos = internalId                      // Not used in the base mailbox.
	m.NewPath = Path{INFINITY, 0, INFINITY} // For static graphs
	m.Mutex = new(sync.RWMutex)             // For dynamic graphs
	return m
}

func (*TP) MailMerge(incoming Mail, sidx uint32, existing *Mail) bool {
	if incoming.Pos == math.MaxUint32 { // Self mail (init)
		existing.NewPath = Path{0, INFINITY, 0}
		return true
	} else if incoming.Pos >= uint32(len(existing.NbrScratch)) {
		// Dynamic: need to reallocate. Expand the size of existing.NbrScratch. New size must fit NbrScratch[pos],
		// So after we expect after that len(NbrScratch) == pos + 1. (or potentially greater if someone else did it first)
		existing.Mutex.Lock()
		prevLen := uint32(len(existing.NbrScratch))
		if incoming.Pos >= prevLen { // check again
			existing.NbrScratch = append(existing.NbrScratch, make([]InboundEntry, ((incoming.Pos+1)-prevLen))...)
		}
		for i := int(prevLen); i < len(existing.NbrScratch); i++ {
			existing.NbrScratch[i] = InboundEntry{Inbound: Path{INFINITY, 0, INFINITY}}
		}
		existing.NbrScratch[incoming.Pos].Inbound = incoming.NewPath
		existing.Mutex.Unlock()
		return true
	}

	prev := &existing.NbrScratch[incoming.Pos].Inbound                            // Only need to lock to write, as retrieve only reads.
	if prev.Start != incoming.NewPath.Start || prev.End != incoming.NewPath.End { // New offer from this edge is different. Note: Ignoring a change in hops...
		existing.Mutex.RLock()
		existing.NbrScratch[incoming.Pos].Inbound = incoming.NewPath
		existing.Mutex.RUnlock()
		return true
	}
	return false
}

func (alg *TP) MailRetrieve(existing *Mail, v *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) (m Mail) {
	if existing.NewPath.Start == 0 && existing.NewPath.End == INFINITY { // I am the source.
		prop.changed = true
		if len(prop.nextWindows) == 0 {
			prop.nextWindows = []Path{existing.NewPath}
		}
		return m
	}
	prop.changed = false
	prop.nextWindows = prop.nextWindows[:0] // Reset the next window array.
	pq := prop.priorityQueue[:0]            // Shortcut reference, and reset the priority queue.

	// First, we will lock the mailbox, and retrieve all of the paths from the vertices that have us as a neighbour.
	// During this first look through all of the data, we will try to aggregate incoming paths as best as possible.
	// However, we may not be able to build the vertex's overall valid set of time windows, because we might receive data about different times.
	// So, we will also sort the inbound data with a priority queue here, and after perform a second pass to fully build our valid window set.
	// Important to note: THIS HAS NOT BEEN OPTIMIZED AT ALL! Every time a vertex receives an update, it recomputes all of its windows, based on the stored data! This should be improved eventually.
	existing.Mutex.Lock()
nbrLoop:
	for nIdx := range existing.NbrScratch {
		inbound := existing.NbrScratch[nIdx].Inbound

		if inbound.Start == INFINITY {
			continue // This edge has not provided us a valid path.
		}
		if len(pq) == 0 { // First path becomes a basis time window, that we will use to try and optimize a little bit.
			pq.Push(inbound)
			continue
		}

		// When some edge tells us about a valid path, lets try to merge it into an existing time window that we have.
		for w := 0; w < len(pq); w++ {
			if inbound.Start == pq[w].Start && inbound.End == pq[w].End {
				// i [-----)
				// w [-----)
				// Inbound is exactly equal, could improve the hops for this window.
				pq[w].Hops = utils.Min(pq[w].Hops, inbound.Hops)
				continue nbrLoop
			} else if inbound.Start <= pq[w].Start && inbound.End >= pq[w].End {
				// i [--[-----)--)
				// w    [-----)
				// Inbound has an earlier/same start, and a later end. Inbound totally dominates.
				pq[w].Start = inbound.Start // Does this have the potential to unsort the heap...?
				pq[w].End = inbound.End
				pq[w].Hops = inbound.Hops // Note a caveat here: this algorithm variant focuses on TIME, not HOPS. So we discard the prior path even if the hops was smaller.
				continue nbrLoop
			} else if inbound.Start > pq[w].Start && inbound.Start <= pq[w].End && inbound.End <= pq[w].End {
				// i    [-----)--)
				// w [-----------)
				// Inbound is a subset of the existing window. Discard. Again, we ignore hops here.
				continue nbrLoop
			} else if EXTEND_TO_EDGE_END && inbound.Start > pq[w].Start && inbound.Start < pq[w].End && inbound.End > pq[w].End {
				// This optimization for merging a window is only possible if we are using the EXTEND_TO_EDGE_END variant.
				// i    [-----)
				// w [-----)
				// Inbound has a later start, but a later end. Aggregate window.
				pq[w].End = inbound.End
				pq[w].Hops = utils.Max(pq[w].Hops, inbound.Hops) // Best effort for calculating hops...
				continue nbrLoop
			}
		}

		// Didn't extend any window, so this is possibly a disjoint window. We will resolve this later, in the second pass.
		pq.Push(inbound)
	}
	existing.Mutex.Unlock() // Done with the lock, we've aggregated the data.

	// Now we begin our second pass over the data, to create a single view of the vertex's valid windows.
	// TODO: A lot of the logic below is similar to above. This should probably be made into some unified function call(s).

	// Start by applying the first entry, since it will be the basis to merge into.
	if len(pq) != 0 {
		prop.nextWindows = append(prop.nextWindows, pq[0])
	}

	// Merge in any other windows. The priority queue is sorted by Start time.
	// We can just iterate over the pq, since it's sorted, so we don't actually need to Pop.
	// TODO: err, do we need to actually check/fix the pq to ensure it is sorted...?
	for w := 1; w < len(pq); w++ {
		prevId := len(prop.nextWindows) - 1
		prevWindow := &prop.nextWindows[prevId] // This is the previous window, which we will compare to (to see if we can merge the next window into it, or if it is confirmed disjoint).
		nextWindow := pq[w]                     // This is the next window, which we are checking to see if it is mergeable or disjoint.

		// Debug check: By the priority queue, the window we are checking should start after/at the previous window's start... right?
		if nextWindow.Start < prevWindow.Start {
			log.Panic().Msg("MailRetrieve: nextWindow.Start < prevWindow.Start. prevWindow: " + utils.V(prevWindow) + " nextWindow: " + utils.V(nextWindow))
		}

		// Check if we can merge the next with the prev window.
		if nextWindow.Start == prevWindow.Start && nextWindow.End == prevWindow.End {
			// next [-----)
			// prev [-----)
			// Next is exactly equal, could improve the hops of the previous.
			prevWindow.Hops = utils.Min(prevWindow.Hops, nextWindow.Hops)
			continue
		} else if nextWindow.Start == prevWindow.Start && nextWindow.End > prevWindow.End {
			// next [-----------)
			// prev [-----)
			// Next has the same start, and a later end.
			prevWindow.End = nextWindow.End
			prevWindow.Hops = nextWindow.Hops // Next totally dominates.
			continue
		} else if nextWindow.Start < prevWindow.End && nextWindow.End <= prevWindow.End {
			// next    [-----)--)
			// prev [-----------)
			// Next starts before the prev window ends.
			// Next ends before/at the prev window ends. So we can ignore this window.
			continue
		} else if EXTEND_TO_EDGE_END && nextWindow.Start >= prevWindow.Start && nextWindow.Start < prevWindow.End && nextWindow.End > prevWindow.End {
			// This optimization for merging a window is only possible if we are using the EXTEND_TO_EDGE_END variant.
			// next    [-----)
			// prev [-----)
			// Next ends after the prev window ends. Aggregate window.
			prevWindow.End = nextWindow.End
			prevWindow.Hops = utils.Max(prevWindow.Hops, nextWindow.Hops)
			continue
		}

		// In the EXTEND_TO_EDGE_END variant, we have confirmed this new window looks something like this:
		// next       [--[---)
		// prev [-----)
		// Otherwise, we have confirmed the new window looks something like this:
		// next    [--[--[---)
		// prev [-----)

		// So, we add this as a confirmed disjoint window.
		prop.nextWindows = append(prop.nextWindows, nextWindow)
		// Here, you can see the impact of the lack of optimization in this code... there's a good chance we just rebuilt the same thing we already knew...
		// Added a new window; so check the previous window (it cannot be altered now) to see if it changed from the previous time we built our overall window view.
		if !prop.changed && (len(prop.Windows) <= prevId || prop.Windows[prevId].Start != prop.nextWindows[prevId].Start || prop.Windows[prevId].End != prop.nextWindows[prevId].End) {
			prop.changed = true
		}
	}

	// Now that we've checked all the windows from the pq, let's do a final check to see if we our "new" view of the vertex's valid windows is different from before.
	if !prop.changed {
		id := len(prop.nextWindows) - 1
		if len(prop.Windows) != len(prop.nextWindows) {
			prop.changed = true // Different size windows: definitely changed.
		} else if len(prop.nextWindows) != 0 && (prop.Windows[id].Start != prop.nextWindows[id].Start || prop.Windows[id].End != prop.nextWindows[id].End) {
			prop.changed = true // Final window differs, so changed. (We check previous windows only above, so here we check the final one.)
		}
	}

	prop.priorityQueue = pq[:0] // Re-assign the priorityQueue reference (in case of a memory realloc).

	return m
}

// Identifies the best "offer" of a valid window to send across an edge.
func (alg *TP) pushWindows(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], _ *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, e *graph.Edge[EdgeProperty], windowStartIdx int) (sent uint64, first int) {
	mailbox, tidx := g.NodeVertexMailbox(e.Didx)
	offer := Path{INFINITY, 0, INFINITY} // Default unreachable.
	first = windowStartIdx               // Default the same.

	// As we move, the vertex window start time increases. We can use this to optimize a little, since both the edges of the vertex and the valid windows are both sorted by time.
	// We try to find the best offer that we can provide to this edge (earliest start time, then latest end time... though, I suppose start time is always the edge start time..?)
	for i := windowStartIdx; i < len(prop.Windows); i++ {
		vrtxWindow := &prop.Windows[i]
		// Start by assuming Edge Start, and Vertex Window End, with one added hop.
		edgeWindow := Path{e.Property.GetTimestamp(), vrtxWindow.End, (vrtxWindow.Hops + 1)}
		if EXTEND_TO_EDGE_END {
			// Extends the window end to the edge End, for just this edge.
			if eEnd := e.Property.GetEndTime(); eEnd != 0 {
				edgeWindow.End = eEnd
			} else {
				edgeWindow.End = INFINITY
			}
		} else {
			// With the non-Extend variant, the end may be earlier than the edge end.
			// Caps window end to min(vrtxWindow.End, eEnd)
			if eEnd := e.Property.GetEndTime(); eEnd != 0 && eEnd < vrtxWindow.End {
				edgeWindow.End = eEnd
			}
		}

		// The vertex window is too early. It cannot traverse this edge.
		// vrtxWindow  [-----)--)
		// edgeWindow           [-----)
		if edgeWindow.Start >= vrtxWindow.End {
			continue // Not a valid path: the edge started after/at the window end.
		}

		// The vertex window is too late. It began after (or at the same time) that the edge began, therefore, it is causally uncorrelated and impassible.
		// Note: Another variant of temporal reachability could still consider this a valid path -- but for this variant, we will consider this invalid. (Also, would need to have an edge cost to avoid cycles.)
		// vrtxWindow     [---
		// edgeWindow  [--[---
		if edgeWindow.Start <= vrtxWindow.Start {
			// Not a valid path: the edge started before/at the window start.
			break // All future windows start later, so we are done (no better offer possible).
		}

		// The vertex window is way too late.
		// vrtxWindow           [-----)
		// edgeWindow  [-----)--)
		// if edgeWindow.End <= vrtxWindow.Start // Err, don't actually need to check this, since it's covered by the previous check.

		// In the EXTEND_TO_EDGE_END variant, we have confirmed this vertex window looks something like this (Possible the window end is extended):
		// vrtxWindow  [----------)
		// edgeWindow      [---)--)--)
		// Otherwise, we have confirmed the vertex window looks something like this (Already capped the end):
		// vrtxWindow  [----------)
		// edgeWindow      [---)--)

		// So, this window is a valid offer for this edge. The start time is the edge start time.

		// A single edge only provides a single offer. (There could be multiple edges between the same vertices, but these edges are treated individually).
		if offer.Start == INFINITY { // First offer.
			first = i
		} else { // Check for a better offer (i.e., an offer with the same start, but a later end).
			if EXTEND_TO_EDGE_END && (edgeWindow.Start == offer.Start) {
				// If using EXTEND_TO_EDGE_END, we might have multiple windows with the same start (but they'll always have the same end).
				// edgeWindow [-----)
				// offer      [-----)
				// Same window. Not useful, but we can take the better hop estimate.
				offer.Hops = utils.Min(offer.Hops, edgeWindow.Hops)
				continue
			} else if (edgeWindow.Start == offer.Start) && (edgeWindow.End > offer.End) {
				// If using EXTEND_TO_EDGE_END, note this won't occur (as the end offer always has the same end, since EXTEND makes the end equal to the edge's end).
				// edgeWindow [-----------)
				// offer      [-----)
				// Same start, later end. This window is better: totally dominates. (Note that we don't care about shorter hop counts in this algorithm variant.)
				offer.End = edgeWindow.End
				offer.Hops = edgeWindow.Hops
				continue
			}
			log.Panic().Msg("unhandled")
		}
		// Set the offer to the (potentially constrained) window that we can provide to this edge.
		offer = edgeWindow
	}

	// Send the offer. We still need to send information even if the "offer" is saying unreachable (since this may be an update).
	mail := Mail{Pos: e.Pos, NewPath: offer}
	if alg.MailMerge(mail, sidx, &mailbox.Inbox) {
		sent += g.EnsureSend(g.UniqueNotification(sidx, graph.Notification[Note]{Target: e.Didx}, mailbox, tidx))
	}
	return sent, first
}

// Important to note: THIS HAS NOT BEEN OPTIMIZED AT ALL! Every time a vertex receives an update, it recomputes all of its windows within the mailbox retrieve step!
func (alg *TP) OnUpdateVertex(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, n graph.Notification[Note], m Mail) (sent uint64) {
	if !prop.changed {
		return 0 // We didn't change anything, nothing to do.
	}
	prop.changed = false

	// Set new view of windows (swap to preserve the array mallocs).
	prop.Windows, prop.nextWindows = prop.nextWindows, prop.Windows

	windowStartIdx := 0
	s := uint64(0)
	// Send to neighbours. Remember, edges are always sorted by start time.
	for i := range src.OutEdges {
		s, windowStartIdx = alg.pushWindows(g, src, prop, n.Target, &src.OutEdges[i], windowStartIdx)
		sent += s
	}
	return sent
}

func (alg *TP) OnEdgeAdd(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, eidxStart int, m Mail) (sent uint64) {
	if prop.changed {
		// Update first. If we already targeted all neighbours, we can skip the rest.
		if sent = alg.OnUpdateVertex(g, gt, src, prop, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
			return sent
		}
	}
	// Target just the new neighbours. Remember, edges are always sorted by start time.
	windowStartIdx := 0
	s := uint64(0)
	for i := eidxStart; i < len(src.OutEdges); i++ {
		s, windowStartIdx = alg.pushWindows(g, src, prop, sidx, &src.OutEdges[i], windowStartIdx)
		sent += s
	}
	return sent
}

func (alg *TP) OnEdgeDel(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note], src *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty, sidx uint32, deleted []graph.Edge[EdgeProperty], m Mail) (sent uint64) {
	if prop.changed {
		// Update first. If we already targeted all neighbours, we can skip the rest -- this is because we still inspect "deleted" edges.
		if sent = alg.OnUpdateVertex(g, gt, src, prop, graph.Notification[Note]{Target: sidx}, m); sent != 0 {
			return sent
		}
	}
	// Target just the edge-ended neighbours.
	for i := range deleted {
		s, _ := alg.pushWindows(g, src, prop, sidx, &deleted[i], 0)
		sent += s
	}
	return sent
}
