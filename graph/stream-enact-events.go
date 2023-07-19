package graph

import (
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// New vertex handler during graph construction. Hooks algorithm events (default constructors).
func NewVertex[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], rawId RawType) (vidx uint32) {
	idx := uint32(len(gt.Vertices))
	vidx = (uint32(gt.Tidx) << THREAD_SHIFT) + idx

	gt.VertexMap[rawId] = vidx

	bucket, pos := idxToBucket(idx)
	if bucket >= uint32(len(gt.VertexMailboxes)) {
		gt.VertexMailboxes = append(gt.VertexMailboxes, new([BUCKET_SIZE]VertexMailbox[M]))
		gt.VertexStructures = append(gt.VertexStructures, new([BUCKET_SIZE]VertexStructure))
	}
	gt.VertexMailboxes[bucket][pos].Inbox = gt.VertexMailboxes[bucket][pos].Inbox.New()

	gt.Vertices = append(gt.Vertices, Vertex[V, E]{})
	gt.Vertices[idx].Property = gt.Vertices[idx].Property.New()

	gt.VertexStructures[bucket][pos] = VertexStructure{
		RawId:      rawId,
		InEventPos: 0,
		PendingIdx: 0,
	}

	v := &gt.Vertices[idx]
	mailbox := &gt.VertexMailboxes[bucket][pos]

	// TODO: Optimize? This is a runtime type check that hits every time. Maybe we can do better.
	if aBVM, ok := any(alg).(AlgorithmBaseVertexMailbox[V, E, M, N]); ok {
		mailbox.Inbox = aBVM.BaseVertexMailbox(v, vidx, &gt.VertexStructures[bucket][pos])
	}

	// TODO: Also runtime type checks below.
	var vidxInit bool
	if !g.NoteInit {
		var mail M
		if !g.SourceInit {
			if algIAM, ok := any(alg).(AlgorithmInitAllMail[V, E, M, N]); ok {
				mail = algIAM.InitAllMail(v, vidx, rawId)
			}
		} else {
			if mail, vidxInit = g.InitMails[rawId]; !vidxInit {
				return vidx
			}
		}

		if newInfo := alg.MailMerge(mail, vidx, &mailbox.Inbox); newInfo {
			mail = alg.MailRetrieve(&mailbox.Inbox, v)
			sent := alg.OnUpdateVertex(g, v, Notification[N]{Target: vidx}, mail)
			gt.MsgSend += sent
		}
	} else {
		var note N
		if !g.SourceInit {
			if algIAN, ok := any(alg).(AlgorithmInitAllNote[V, E, M, N]); ok {
				note = algIAN.InitAllNote(v, vidx, rawId)
			}
		} else {
			if note, vidxInit = g.InitNotes[rawId]; !vidxInit {
				return vidx
			}
		}
		n := Notification[N]{Target: vidx, Note: note}
		mailbox, tidx := g.NodeVertexMailbox(vidx)
		sent := g.EnsureSend(g.ActiveNotification(vidx, n, mailbox, tidx))
		g.GraphThreads[tidx].MsgSend += sent
	}
	return vidx
}

// Checks the incoming from-emit queue, and passes anything to the remitter.
func checkToRemit[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N]) (closed bool, count uint64) {
	var ok bool
	var didx uint32
	var event TopologyEvent[E]

	spaceAvailable := gt.ToRemitQueue.EnqCheckRange()
	for ; count < spaceAvailable; count++ {
		if event, ok = gt.FromEmitQueue.Accept(); !ok {
			break
		}
		if didx, ok = gt.VertexMap[event.DstRaw]; !ok {
			didx = NewVertex(alg, g, gt, event.DstRaw)
		}

		pos := ^uint32(0)
		if event.EventType() == ADD {
			vs := gt.VertexStructure(didx)
			pos = vs.InEventPos
			vs.InEventPos++
			gt.NumInEvents++ // Thread total; unused.
		}

		// event.EventIdx() // Unused. Could view the global event count here.

		// Will always succeed (range check is lte current space)
		gt.ToRemitQueue.Offer(RawEdgeEvent[E]{TypeAndEventIdx: event.TypeAndEventIdx, SrcRaw: event.SrcRaw, DstRaw: event.DstRaw, Edge: Edge[E]{Property: event.EdgeProperty, Didx: didx, Pos: pos}})
	}
	if spaceAvailable != 0 && count == 0 {
		closed = gt.FromEmitQueue.IsClosed()
	}
	return closed, count
}

// Injects expired edges into the topology event buffer. To be done after the topology event buffer has been remapped with internal source ids.
func InjectExpired[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], changeCount int, uniqueCount uint64, delOnExpire uint64) (numInjected int, newUniqueCount uint64) {
	latestTime := gt.TopologyEventBuff[changeCount-1].Edge.Property.GetTimestamp() // Unless there are out of order events...?

	if latestTime == 0 && g.warnZeroTimestamp == 0 && atomic.CompareAndSwapUint64(&g.warnZeroTimestamp, 0, 1) {
		log.Warn().Msg("WARNING: detected a 0 for timestamp event(s). Please check -pt option.")
	}

	for i := 0; i < changeCount; i++ {
		if gt.TopologyEventBuff[i].EventType() != ADD {
			continue
		}
		// Adjust the copied edge.
		futureDelete := &(gt.ExpiredEdges[len(gt.ExpiredEdges)-changeCount+i].Second)
		// TODO: this expiry copies the event idx of the original. Perhaps this whole process should be move to the emitter...
		futureDelete.TypeAndEventIdx = ((futureDelete.TypeAndEventIdx >> EVENT_TYPE_BITS) << EVENT_TYPE_BITS) | uint64(DEL)
		thisEdgeTime := futureDelete.Edge.Property.GetTimestamp()
		if (thisEdgeTime > latestTime) && g.warnOutOfOrderInput == 0 && atomic.CompareAndSwapUint64(&g.warnOutOfOrderInput, 0, 1) {
			log.Warn().Msg("WARNING: detected out of order timestamps in events. This may slow down the edge expiry system.")
			log.Warn().Msg("ts: " + utils.V(latestTime) + " < " + utils.V(thisEdgeTime) + " unix: " + utils.V(time.Unix(int64(latestTime), 0)) + " < " + utils.V(time.Unix(int64(thisEdgeTime), 0)))
			log.Warn().Msg("pos: " + utils.V(i) + " latestPos " + utils.V(changeCount-1))
			log.Panic().Msg("ERROR: Out of order timestamps in events is currently disabled. For now use: lp-edgelist-tools -sort")
		}
		EP(&futureDelete.Edge.Property).ReplaceTimestamp(thisEdgeTime + delOnExpire)
		if g.Options.TimeRange {
			EP(&futureDelete.Edge.Property).ReplaceEndTime(thisEdgeTime + delOnExpire)
		}
	}
	// TODO: Out-of-order timestamp input needs more considerations... sorting here and edges later is ok, but does not guarantee some desired properties.
	if g.warnOutOfOrderInput != 0 {
		gte := gt.ExpiredEdges // Sort expired edges by timestamp
		sort.SliceStable(gte, func(i, j int) bool {
			return gte[i].Second.Edge.Property.GetTimestamp() < gte[j].Second.Edge.Property.GetTimestamp()
		})
	}

	for ; ; numInjected++ { // Check if we need to inject expired edges, and inject all that are expired
		if len(gt.ExpiredEdges) == numInjected {
			break
		}
		nextDelete := gt.ExpiredEdges[numInjected]
		ts := nextDelete.Second.Edge.Property.GetTimestamp()
		if ts > latestTime {
			break
		}
		// TODO: here we make the start timestamp of the injected delete event the same as the add event...
		// This will need more consideration if/when we wish to target edges more specifically / efficiently.
		EP(&nextDelete.Second.Edge.Property).ReplaceTimestamp(ts - delOnExpire)

		uniqueCount = gt.checkInsertPending(nextDelete.First, uint32(changeCount+numInjected), uniqueCount)

		if (changeCount + numInjected) >= len(gt.TopologyEventBuff) {
			gt.TopologyEventBuff = append(gt.TopologyEventBuff, nextDelete.Second)
		} else {
			gt.TopologyEventBuff[changeCount+numInjected] = nextDelete.Second
		}
	}
	return numInjected, uniqueCount
}

// Handles the (unique) insertion of the indices into the pending buffer.
func (gt *GraphThread[V, E, M, N]) checkInsertPending(sidx uint32, pending uint32, uniqueCount uint64) uint64 {
	vs := gt.VertexStructure(sidx)
	if vPendIdx := vs.PendingIdx; vPendIdx < (gt.NumUnique) {
		vs.PendingIdx = uniqueCount + (gt.NumUnique)
		// Check if we need to grow the buffer
		if len(gt.VertexPendingBuff) <= int(uniqueCount) {
			newPend := append(make([]uint32, 0, 256), []uint32{sidx, pending}...) // First entry is the source vertex.
			gt.VertexPendingBuff = append(gt.VertexPendingBuff, newPend)
			// log.Debug().Msg("EnactTopologyEvents: gt.VertexPendingBuff grown")
		} else {
			// Insert into existing buffer (slot is 'empty' right now but maintains the array backing)
			gt.VertexPendingBuff[uniqueCount] = append(gt.VertexPendingBuff[uniqueCount], []uint32{sidx, pending}...) // First entry is the source vertex.
		}
		uniqueCount++
	} else {
		// Non-unique, append to existing position.
		gt.VertexPendingBuff[vPendIdx-(gt.NumUnique)] = append(gt.VertexPendingBuff[vPendIdx-(gt.NumUnique)], pending)
	}
	return uniqueCount
}

// Main function to enact topology events. Will look through the topology event buffer and apply changes to the graph.
func EnactTopologyEvents[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], changeCount int, delOnExpire uint64) (addEvents uint32, delEvents uint32) {
	numInjected := 0
	uniqueCount := uint64(0)
	canCheckTimestamp := (g.Options.TimestampPos != 0) || FAKE_TIMESTAMP
	var ok bool
	var sidx uint32

	for i := 0; i < changeCount; i++ {
		if sidx, ok = gt.VertexMap[gt.TopologyEventBuff[i].SrcRaw]; !ok {
			sidx = NewVertex(alg, g, gt, gt.TopologyEventBuff[i].SrcRaw)
		}

		uniqueCount = gt.checkInsertPending(sidx, uint32(i), uniqueCount)

		if delOnExpire > 0 && gt.TopologyEventBuff[i].EventType() == ADD {
			gt.ExpiredEdges = append(gt.ExpiredEdges, utils.Pair[uint32, RawEdgeEvent[E]]{First: sidx, Second: gt.TopologyEventBuff[i]})
		}

		// gt.TopologyEventBuff[i].EventIdx() // Unused. Could view the global event count here.
	}

	if delOnExpire > 0 { // (These are now checked after the first look through)
		numInjected, uniqueCount = InjectExpired[EP](g, gt, changeCount, uniqueCount, delOnExpire)
	}

	gt.NumUnique += uniqueCount

	sent := uint64(0)

	// Next, range over the new graph events. Here we range over vertices.
	for u := uint64(0); u < uniqueCount; u++ {
		pendIdx := gt.VertexPendingBuff[u]
		sidx = pendIdx[0] // First entry is the source vertex internalId.
		src, mailbox := gt.VertexAndMailbox(sidx)

		// Here we loop over changes to a vertices edges.
		for idx := 1; idx < len(pendIdx); {
			// First: gather any consecutive edge ADDs. This is because we wish to aggregate them.
			eidxStart := len(src.OutEdges)
			for ; idx < len(pendIdx); idx++ {
				change := &gt.TopologyEventBuff[pendIdx[idx]]
				if change.EventType() == ADD {
					//log.Info().Msg("T[" + utils.F("%02d", gt.Tidx) + "] Found add  edge: " + change.StringRemapped() + " ; before edge list: " + utils.V(src.OutEdges))
					src.OutEdges = append(src.OutEdges, change.Edge)
				} else {
					break // Was not an add; we will break early and address the next changes after we finish processing the adds.
				}
			}
			// From the gathered set of consecutive adds, apply them.
			if len(src.OutEdges) > eidxStart {
				mail := alg.MailRetrieve(&mailbox.Inbox, src)
				sent += alg.OnEdgeAdd(g, src, sidx, eidxStart, mail)
			}
			addEvents += uint32(len(src.OutEdges) - eidxStart)

			// If we didn't finish, it means we hit a delete. Address any consecutive deletes here.
			if idx < len(pendIdx) {
				var deletedEdges []Edge[E] // TODO: cache this slice
				deleteIdx := 0
				if !g.Options.TimeRange {
					var deleteEdgesIdx []int // TODO: cache this slice
					for ; idx < len(pendIdx); idx++ {
						change := &gt.TopologyEventBuff[pendIdx[idx]]
						if change.EventType() == DEL {
							didx := change.Edge.Didx
							if src.OutEdges[deleteIdx].Didx == didx && (src.OutEdges[deleteIdx].Pos&(1<<31) == 0) {
								// Fast path, if its the first edge (and continues to be the first edge).
								// Edges are ordered by event, so fast if its an expiry or otherwise targets oldest (time window).
								deleteEdgesIdx = append(deleteEdgesIdx, deleteIdx)
								src.OutEdges[deleteIdx].Pos |= (1 << 31) // Flag the Pos to indicate marked for delete.
							} else {
								if g.warnSelectDelete == 0 && atomic.CompareAndSwapUint64(&g.warnSelectDelete, 0, 1) {
									log.Warn().Msg("WARNING: Detected a targeted/direct deletion. This has considerations for a multigraph.")
									log.Warn().Msg("-- It will delete the **first** instance of the matching destination edge (FIFO).")
									log.Warn().Msg("-- Edges are maintained in stable **temporal order** if given timestamps, otherwise by stream **event order** (ideally this is equivalent ordering).")
									log.Warn().Msg("WARNING: Currently doing a linear search for direct deletion, this is not optimized yet!!")
								}
								found := false
								// Backup method, linear search.
								// TODO: Sort if detected input is unordered by timestamp here?
								// Ideally the operator gives more information about the edge to delete (e.g. give the original add timestamp / eventIdx). This would allow binary search.
								for eidx := range src.OutEdges {
									if src.OutEdges[eidx].Didx == didx && (src.OutEdges[eidx].Pos&(1<<31) == 0) { // Ensure we don't try to delete the same edge twice.
										src.OutEdges[eidx].Pos |= (1 << 31) // Flag the Pos to indicate marked for delete.
										deleteEdgesIdx = append(deleteEdgesIdx, eidx)
										found = true
										break
									}
								}
								if !found {
									log.Error().Msg("Edge list: " + utils.V(src.OutEdges))
									idx, _ := InternalExpand(sidx)
									log.Panic().Msg("T[" + utils.F("%02d", gt.Tidx) + "] Cannot delete an edge for idx " + utils.V(idx) + " that was not found:   " + change.String())
								}
							}
							//log.Info().Msg("T[" + utils.F("%02d", gt.Tidx) + "] Found del  edge: " + change.StringRemapped() + " ; before edge list: " + utils.V(src.OutEdges))
							deleteIdx++
						} else {
							break // Out of consecutive deletes, so break the for, and process them.
						}
					}
					if g.warnSelectDelete == 0 {
						// Easiest path! If we are not selecting deletes, then we are expiring edges; in this case, the oldest edges are always first.
						// Expired edges are always the newest, so we can just adjust with slice headers.
						deletedEdges = src.OutEdges[:deleteIdx]
						src.OutEdges = src.OutEdges[deleteIdx:]
					} else if canCheckTimestamp {
						// Alright path. If we have timestamps, then we can move (sort) the deleted edges to the end.
						// TODO: If the input was out of order, a consideration is that the edgelist gets sorted here. This becomes in-between the goal of preserving input order vs timestamp order.
						// Should keep deleted edges in order too. Move them to the back of the list.
						// TODO: Filter with a temp array probably faster.
						for i := range deleteEdgesIdx {
							flaggedTime := src.OutEdges[deleteEdgesIdx[i]].Property.GetTimestamp() | (1 << 63) // Set MSB flag, for sorting. Preserve real timestamp as well.
							EP(&src.OutEdges[deleteEdgesIdx[i]].Property).ReplaceTimestamp(flaggedTime)
						}
						// Stable, to preserve order presented (in case of same timestamp).
						sortSrcEdges := src.OutEdges
						sort.SliceStable(sortSrcEdges, func(i, j int) bool {
							return sortSrcEdges[i].Property.GetTimestamp() < sortSrcEdges[j].Property.GetTimestamp()
						})
						deletedEdges = src.OutEdges[len(src.OutEdges)-deleteIdx:]
						for d := range deletedEdges { // Clear the MSB flags.
							EP(&deletedEdges[d].Property).ReplaceTimestamp(deletedEdges[d].Property.GetTimestamp() & ((1 << 63) - 1))
						}
						src.OutEdges = src.OutEdges[:len(src.OutEdges)-deleteIdx]
					} else {
						// No timestamps to sort against... Note this is worse than timestamp sorting,
						// since the sort function won't sort well (it sorting by just the MSB flag and preserving order).
						// Should keep deleted edges in order too. Move them to the back of the list.
						// TODO: Filter with a temp array probably faster.
						sortSrcEdges := src.OutEdges
						sort.SliceStable(sortSrcEdges, func(i, j int) bool {
							return (sortSrcEdges[i].Pos & (1 << 31)) < (sortSrcEdges[j].Pos & (1 << 31))
						})
						deletedEdges = src.OutEdges[len(src.OutEdges)-deleteIdx:]
						src.OutEdges = src.OutEdges[:len(src.OutEdges)-deleteIdx]
					}
					for d := range deletedEdges { // Clear the Pos flag. (Algorithm should get the real Pos.)
						deletedEdges[d].Pos &= (1 << 31) - 1
					}

				} else { // Time range edges. They have an end / termination time -- rather than being "deleted".
					for ; idx < len(pendIdx); idx++ {
						change := &gt.TopologyEventBuff[pendIdx[idx]]
						if change.EventType() == DEL {
							didx := change.Edge.Didx
							for src.OutEdges[deleteIdx].Property.GetEndTime() != 0 {
								deleteIdx++ // Skip any edges that are already ended.
							}
							if src.OutEdges[deleteIdx].Didx == didx {
								// Fast path, if its the first un-ended edge (and continues to be the first).
								// Edges are ordered by start time, so fast if its an expiry or otherwise targets oldest (time window).
								EP(&src.OutEdges[deleteIdx].Property).ReplaceEndTime(change.Edge.Property.GetEndTime())
								deletedEdges = append(deletedEdges, src.OutEdges[deleteIdx])
							} else {
								// TODO: Select delete -- will be a binary search for the timestamp.
								log.Panic().Msg("Not implemented yet.")
							}
							deleteIdx++
						} else {
							break // Out of consecutive deletes, so break the for, and process them.
						}
					}
				}
				// From the gathered set of consecutive deletes, apply them.
				mail := alg.MailRetrieve(&mailbox.Inbox, src)
				sent += alg.OnEdgeDel(g, src, sidx, deletedEdges, mail)
				delEvents += uint32(len(deletedEdges))
			} // Addressed the delete(s), continue the loop (go back to checking for consecutive adds).
		}
		// Reset the pending list for this position (without freeing the buffer).
		gt.VertexPendingBuff[u] = gt.VertexPendingBuff[u][:0]
	}

	if numInjected > 0 {
		gt.ExpiredEdges = gt.ExpiredEdges[numInjected:] // Deque might be better, but would be harder to sort.
	}
	gt.MsgSend += sent
	return addEvents, delEvents
}

// For testing. Injects deletes into a given topology change list (of implicit adds), but retains the final structure.
func InjectDeletesRetainFinalStructure[E EPI[E]](sc []TopologyEvent[E], chance float64) (returnSC []TopologyEvent[E]) {
	availableAdds := make([]TopologyEvent[E], len(sc))
	var previousAdds []TopologyEvent[E]

	copy(availableAdds, sc)
	utils.Shuffle(availableAdds)

	for eventIdx := uint64(0); len(availableAdds) > 0; eventIdx++ {
		var current TopologyEvent[E]
		if len(previousAdds) > 0 && rand.Float64() < chance {
			current, previousAdds = utils.RemoveRandomElement(previousAdds)
			availableAdds = append(availableAdds, current)
			current.TypeAndEventIdx = uint64(DEL)
		} else {
			current, availableAdds = utils.RemoveRandomElement(availableAdds)
			previousAdds = append(previousAdds, current)
		}
		current.TypeAndEventIdx = (eventIdx << EVENT_TYPE_BITS) | (current.TypeAndEventIdx & EVENT_TYPE_MASK)
		returnSC = append(returnSC, current)
	}
	return returnSC
}
