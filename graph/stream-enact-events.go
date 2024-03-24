package graph

import (
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

func CreateVertexIfNeeded[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], rawId RawType, eventIdx uint64, vidx uint32) {
	idx, tidx := InternalExpand(vidx)
	if tidx != uint32(gt.Tidx) {
		log.Panic().Msg("")
	}
	bucket, pos := idxToBucket(idx)
	if int(idx) >= len(gt.Vertices) || bucket >= uint32(len(gt.VertexMailboxes)) || gt.VertexStructures[bucket][pos].CreateEvent == math.MaxUint64 {
		newVertex(alg, g, gt, rawId, eventIdx, vidx)
	}
	g.NodeVertex(vidx) // REMOVE
}

// New vertex handler during graph construction. Hooks algorithm events (default constructors).
func newVertex[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], rawId RawType, eventIdx uint64, vidx uint32) {
	idx, _ := InternalExpand(vidx)

	if int(idx) >= len(gt.Vertices) {
		gt.Vertices = append(gt.Vertices, make([]Vertex[V, E], int(idx)+1-len(gt.Vertices))...)
	}

	bucket, pos := idxToBucket(idx)
	for bucket >= uint32(len(gt.VertexMailboxes)) {
		gt.VertexMailboxes = append(gt.VertexMailboxes, new([BUCKET_SIZE]VertexMailbox[M]))
		gt.VertexStructures = append(gt.VertexStructures, new([BUCKET_SIZE]VertexStructure))
		gt.VertexProperties = append(gt.VertexProperties, new([BUCKET_SIZE]V))
		for p := 0; p < BUCKET_SIZE; p++ {
			gt.VertexStructures[len(gt.VertexMailboxes)-1][p].CreateEvent = math.MaxUint64
		}
	}
	gt.VertexMailboxes[bucket][pos].Inbox = gt.VertexMailboxes[bucket][pos].Inbox.New()
	gt.VertexProperties[bucket][pos] = gt.VertexProperties[bucket][pos].New()

	gt.VertexStructures[bucket][pos] = VertexStructure{
		PendingIdx:  0,
		CreateEvent: eventIdx,
		InEventPos:  0,
		RawId:       rawId,
	}

	v := &gt.Vertices[idx] // gt.Vertices should have been resized inCreateVertexIfNeeded
	mailbox := &gt.VertexMailboxes[bucket][pos]

	// TODO: Optimize? This is a runtime type check that hits every time. Maybe we can do better.
	if aBVM, ok := any(alg).(AlgorithmBaseVertexMailbox[V, E, M, N]); ok {
		mailbox.Inbox = aBVM.BaseVertexMailbox(v, gt.VertexProperty(vidx), vidx, &gt.VertexStructures[bucket][pos])
	}

	// TODO: Also runtime type checks below.
	var vidxInit bool
	if !g.NoteInit {
		var mail M
		if !g.SourceInit {
			if algIAM, ok := any(alg).(AlgorithmInitAllMail[V, E, M, N]); ok {
				mail = algIAM.InitAllMail(v, gt.VertexProperty(vidx), vidx, rawId)
			}
		} else {
			if mail, vidxInit = g.InitMails[rawId]; !vidxInit {
				return
			}
		}

		if newInfo := alg.MailMerge(mail, vidx, &mailbox.Inbox); newInfo {
			mail = alg.MailRetrieve(&mailbox.Inbox, v, gt.VertexProperty(vidx))
			sent := alg.OnUpdateVertex(g, gt, v, gt.VertexProperty(vidx), Notification[N]{Target: vidx}, mail)
			gt.MsgSend += sent
		}
	} else {
		var note N
		if !g.SourceInit {
			if algIAN, ok := any(alg).(AlgorithmInitAllNote[V, E, M, N]); ok {
				note = algIAN.InitAllNote(v, gt.VertexProperty(vidx), vidx, rawId)
			}
		} else {
			if note, vidxInit = g.InitNotes[rawId]; !vidxInit {
				return
			}
		}
		n := Notification[N]{Target: vidx, Note: note}
		mailbox, tidx := g.NodeVertexMailbox(vidx)
		sent := g.EnsureSend(g.ActiveNotification(vidx, n, mailbox, tidx))
		g.GraphThreads[tidx].MsgSend += sent
	}
}

// Checks the incoming from-emit queue, and passes anything to the remitter.
func checkToRemit[V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], onInEdgeAddFunc func(*Graph[V, E, M, N], *GraphThread[V, E, M, N], *Vertex[V, E], *V, uint32, uint32, *TopologyEvent[E])) (closed bool, count uint64) {
	spaceAvailable := gt.ToRemitQueue.EnqCheckRange()
	for ; count < spaceAvailable; count++ {
		event, ok := gt.FromEmitQueue.Accept()
		if !ok {
			break
		}
		didx := event.Edge.Didx
		CreateVertexIfNeeded(alg, g, gt, event.DstRaw, event.EventIdx(), didx)

		pos := ^uint32(0)
		if event.EventType() == ADD {
			vs := gt.VertexStructure(didx)
			pos = vs.InEventPos
			if onInEdgeAddFunc != nil {
				tEvent := TopologyEvent[E]{
					TypeAndEventIdx: event.TypeAndEventIdx,
					SrcRaw:          event.SrcRaw,
					DstRaw:          event.DstRaw,
					EdgeProperty:    event.Edge.Property,
				}
				onInEdgeAddFunc(g, gt, gt.Vertex(didx), gt.VertexProperty(didx), didx, pos, &tEvent)
			}
			vs.InEventPos++
			gt.NumInEvents++ // Thread total; unused.
		}

		// Will always succeed (range check is lte current space)
		event.Edge.Pos = pos
		gt.ToRemitQueue.Offer(event)
	}
	if spaceAvailable != 0 && count == 0 {
		closed = gt.FromEmitQueue.IsClosed()
	}
	return closed, count
}

// Injects expired edges into the topology event buffer. To be done after the topology event buffer has been remapped with internal source ids.
func InjectExpired[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any](g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], changeCount uint64, uniqueCount uint64, delOnExpire uint64) (newUniqueCount uint64) {
	// TODO: Vertex Add events may not have edge timestamps
	latestTime := gt.TopologyEventBuff[changeCount-1].Edge.Property.GetTimestamp() // Unless there are out of order events...?

	if latestTime == 0 && g.warnZeroTimestamp == 0 && atomic.CompareAndSwapUint64(&g.warnZeroTimestamp, 0, 1) {
		log.Warn().Msg("WARNING: detected a 0 for timestamp event(s). Please check -pt option.")
	}

	for i := uint64(0); i < changeCount; i++ {
		if gt.TopologyEventBuff[i].EventType() != ADD {
			continue
		}
		// TODO: Could randomize if edges are deleted. Though, if we randomize the end time, note we would need to sort the expired edges (or use heap/pq).
		//if rand.Float64() > 0.5 {
		//	continue
		//}

		// Adjust the copied edge.
		futureDelete := &(gt.ExpiredEdges[(uint64(len(gt.ExpiredEdges)) - changeCount + i)].Second)
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

	expiredIndex := uint64(0)
	for ; expiredIndex < uint64(len(gt.ExpiredEdges)); expiredIndex++ { // Check if we need to inject expired edges, and inject all that are expired
		nextDelete := &gt.ExpiredEdges[expiredIndex]
		if (nextDelete.Second.TypeAndEventIdx & EVENT_TYPE_MASK) != uint64(DEL) {
			continue // Must have decided not to change it to a delete.
		}
		ts := nextDelete.Second.Edge.Property.GetTimestamp()
		if ts > latestTime {
			break
		}
		// TODO: here we make the start timestamp of the injected delete event the same as the add event...
		// This will need more consideration if/when we wish to target edges more specifically / efficiently.
		EP(&nextDelete.Second.Edge.Property).ReplaceTimestamp(ts - delOnExpire)

		uniqueCount = gt.checkInsertPending(nextDelete.First, uint32(changeCount+expiredIndex), uniqueCount)

		if (changeCount + expiredIndex) >= uint64(len(gt.TopologyEventBuff)) {
			gt.TopologyEventBuff = append(gt.TopologyEventBuff, nextDelete.Second)
		} else {
			gt.TopologyEventBuff[changeCount+expiredIndex] = nextDelete.Second
		}
	}
	gt.ExpiredEdges = gt.ExpiredEdges[expiredIndex:] // Deque might be better, but would be harder to sort.
	return uniqueCount
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
func EnactTopologyEvents[EP EPP[E], V VPI[V], E EPI[E], M MVI[M], N any, A Algorithm[V, E, M, N]](alg A, g *Graph[V, E, M, N], gt *GraphThread[V, E, M, N], changeCount uint64, delOnExpire uint64) (addEvents uint32, delEvents uint32) {
	uniqueCount := uint64(0)
	canCheckTimestamp := (g.Options.TimestampPos != 0) || g.Options.LogicalTime

	for i := uint64(0); i < changeCount; i++ {
		event := &gt.TopologyEventBuff[i]
		sidx := event.SrcIdx
		CreateVertexIfNeeded(alg, g, gt, event.SrcRaw, event.EventIdx(), sidx)

		uniqueCount = gt.checkInsertPending(sidx, uint32(i), uniqueCount)

		if delOnExpire > 0 && event.EventType() == ADD {
			gt.ExpiredEdges = append(gt.ExpiredEdges, utils.Pair[uint32, InternalTopologyEvent[E]]{First: sidx, Second: *event})
		}
	}
	gt.AtEvent = gt.TopologyEventBuff[changeCount-1].EventIdx()

	if delOnExpire > 0 { // (These are now checked after the first look through)
		uniqueCount = InjectExpired[EP](g, gt, changeCount, uniqueCount, delOnExpire)
	}

	gt.NumUnique += uniqueCount

	sent := uint64(0)

	// Next, range over the new graph events. Here we range over vertices.
	for u := uint64(0); u < uniqueCount; u++ {
		pendIdx := gt.VertexPendingBuff[u]
		sidx := pendIdx[0] // First entry is the source vertex internalId.
		src, mailbox := gt.VertexAndMailbox(sidx)
		prop := gt.VertexProperty(sidx)

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
				mail := alg.MailRetrieve(&mailbox.Inbox, src, prop)
				sent += alg.OnEdgeAdd(g, gt, src, prop, sidx, eidxStart, mail)
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
								deleteIdx++
							} else {
								// TODO: Select delete -- should be a binary search for the timestamp.
								// But I'm lazy... so for now do a linear search to find the matching edge.
								if g.warnSelectDelete == 0 && atomic.CompareAndSwapUint64(&g.warnSelectDelete, 0, 1) {
									log.Warn().Msg("WARNING: Detected a targeted/direct deletion. This isn't optimized yet!! It should be a binary search.")
								}
								found := false
								for eidx := range src.OutEdges {
									if src.OutEdges[eidx].Didx == didx && src.OutEdges[eidx].Property.GetEndTime() == 0 && src.OutEdges[eidx].Property.GetTimestamp() == change.Edge.Property.GetTimestamp() {
										EP(&src.OutEdges[eidx].Property).ReplaceEndTime(change.Edge.Property.GetEndTime())
										deletedEdges = append(deletedEdges, src.OutEdges[eidx])
										found = true
										break
									}
								}
								if !found {
									log.Error().Msg("Edge list: " + utils.V(src.OutEdges))
									idx, t := InternalExpand(sidx)
									log.Panic().Msg("T[" + utils.F("%02d", t) + "] Cannot delete an edge for idx " + utils.V(idx) + " that was not found: " + change.String())
								}
							}
						} else {
							break // Out of consecutive deletes, so break the for, and process them.
						}
					}
				}
				// From the gathered set of consecutive deletes, apply them.
				mail := alg.MailRetrieve(&mailbox.Inbox, src, prop)
				sent += alg.OnEdgeDel(g, gt, src, prop, sidx, deletedEdges, mail)
				delEvents += uint32(len(deletedEdges))
			} // Addressed the delete(s), continue the loop (go back to checking for consecutive adds).
		}
		// Reset the pending list for this position (without freeing the buffer).
		gt.VertexPendingBuff[u] = gt.VertexPendingBuff[u][:0]
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
