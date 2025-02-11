package main

import (
	"math"
	"sort"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
	"github.com/rs/zerolog/log"
)

const COMPARE_SINGLE_TRAVERSAL = true

func (*TP) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	oracleValuesS := make([]uint64, g.NodeVertexCount())
	givenGValuesS := make([]uint64, g.NodeVertexCount())

	oracleValuesE := make([]uint64, g.NodeVertexCount())
	givenGValuesE := make([]uint64, g.NodeVertexCount())

	oracleValuesH := make([]uint64, g.NodeVertexCount())
	givenGValuesH := make([]uint64, g.NodeVertexCount())

	numEdges := g.NodeParallelFor(func(ordinalStart, _ uint32, givenGt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		oracleGt := &oracle.GraphThreads[givenGt.Tidx]
		for i := uint32(0); i < uint32(len(givenGt.Vertices)); i++ {
			givenProp := givenGt.VertexProperty(i)
			oracleProp := oracleGt.VertexProperty(i)

			if len(oracleProp.Windows) != len(givenProp.Windows) {
				log.Panic().Msg("T[" + utils.F("%02d", givenGt.Tidx) + "] Non-equivalent windows of vRawID: " + utils.V(givenGt.VertexRawID(i)) +
					"\nOracle: " + utils.V(oracleProp.Windows) +
					"\nGiven:  " + utils.V(givenProp.Windows))
			}

			for w := 0; w < len(oracleProp.Windows); w++ {
				oracleValuesS[ordinalStart+i] = oracleProp.Windows[w].Start
				givenGValuesS[ordinalStart+i] = givenProp.Windows[w].Start
				oracleValuesE[ordinalStart+i] = oracleProp.Windows[w].End
				givenGValuesE[ordinalStart+i] = givenProp.Windows[w].End
				oracleValuesH[ordinalStart+i] = oracleProp.Windows[w].Hops
				givenGValuesH[ordinalStart+i] = givenProp.Windows[w].Hops

				if oracleValuesS[ordinalStart+i] != givenGValuesS[ordinalStart+i] {
					log.Panic().Msg("T[" + utils.F("%02d", givenGt.Tidx) + "] w " + utils.V(w) +
						"\nStart of " + utils.V(givenGt.VertexRawID(i)) + " : " + " Oracle: " + utils.V(oracleValuesS[ordinalStart+i]) + " Given: " + utils.V(givenGValuesS[ordinalStart+i]) +
						"\nEnd   of " + utils.V(givenGt.VertexRawID(i)) + " : " + " Oracle: " + utils.V(oracleValuesE[ordinalStart+i]) + " Given: " + utils.V(givenGValuesE[ordinalStart+i]) +
						"\nHops  of " + utils.V(givenGt.VertexRawID(i)) + " : " + " Oracle: " + utils.V(oracleValuesH[ordinalStart+i]) + " Given: " + utils.V(givenGValuesH[ordinalStart+i]) +
						"\nNon-equivalent window values (start)." +
						"\nOracle: " + utils.V(oracleProp.Windows) +
						"\nGiven:  " + utils.V(givenProp.Windows))
				}
				if oracleValuesE[ordinalStart+i] != givenGValuesE[ordinalStart+i] {
					log.Panic().Msg("T[" + utils.F("%02d", givenGt.Tidx) + "] w " + utils.V(w) +
						"\nStart of " + utils.V(givenGt.VertexRawID(i)) + " : " + " Oracle: " + utils.V(oracleValuesS[ordinalStart+i]) + " Given: " + utils.V(givenGValuesS[ordinalStart+i]) +
						"\nEnd   of " + utils.V(givenGt.VertexRawID(i)) + " : " + " Oracle: " + utils.V(oracleValuesE[ordinalStart+i]) + " Given: " + utils.V(givenGValuesE[ordinalStart+i]) +
						"\nHops  of " + utils.V(givenGt.VertexRawID(i)) + " : " + " Oracle: " + utils.V(oracleValuesH[ordinalStart+i]) + " Given: " + utils.V(givenGValuesH[ordinalStart+i]) +
						"\nNon-equivalent window values (end)." +
						"\nOracle: " + utils.V(oracleProp.Windows) +
						"\nGiven:  " + utils.V(givenProp.Windows))
				}
			}
		}
		return int(givenGt.NumEdges)
	})

	log.Info().Msg("VertexCount: " + utils.V(g.NodeVertexCount()) + " EdgeCount: " + utils.V(numEdges) + " Diffs:")
	avgL1Diff, medianL1Diff, percentile95L1 := utils.ResultCompare(oracleValuesS, givenGValuesS, 0)
	log.Info().Msg("Start: AvgL1Diff " + utils.F("%.3e", avgL1Diff) + " MedianL1Diff " + utils.F("%.3e", medianL1Diff) + " 95pL1Diff " + utils.F("%.3e", percentile95L1))
	avgL1Diff, medianL1Diff, percentile95L1 = utils.ResultCompare(oracleValuesE, givenGValuesE, 0)
	log.Info().Msg("End:   AvgL1Diff " + utils.F("%.3e", avgL1Diff) + " MedianL1Diff " + utils.F("%.3e", medianL1Diff) + " 95pL1Diff " + utils.F("%.3e", percentile95L1))
}

func (*TP) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	// Check that all windows are valid.
	g.NodeParallelFor(func(ordinalStart, _ uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		tidx := gt.Tidx
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vertex := &gt.Vertices[i]
			prop := gt.VertexProperty(i)
			for wIdx := range prop.Windows {
				for eidx := range vertex.OutEdges {
					eS := vertex.OutEdges[eidx].Property.GetTimestamp()
					eE := vertex.OutEdges[eidx].Property.GetEndTime()
					if eE == 0 {
						eE = math.MaxUint64
					}
					windowProvideStart := prop.Windows[wIdx].Start
					if eS <= windowProvideStart {
						continue // Not possible to traverse (edge starts before window)
					}
					windowProvideStart = eS

					if eE < windowProvideStart {
						continue // Not possible to traverse (edge ends before window starts)
					}
					windowProvideEnd := prop.Windows[wIdx].End
					if windowProvideEnd > eE {
						windowProvideEnd = eE
					}
					if eS >= windowProvideEnd {
						continue // Not possible to traverse (edge starts after/as window ends)
					}

					targetWindows := g.NodeVertexProperty(vertex.OutEdges[eidx].Didx).Windows
					for tWIdx := range targetWindows {
						targetStart := targetWindows[tWIdx].Start
						targetEnd := targetWindows[tWIdx].End

						if prop.Windows[wIdx].End <= targetStart {
							continue // Our window ended before their window started.
						}
						if prop.Windows[wIdx].End < targetEnd {
							continue // Our window ended before their window ended. Our window cannot provide this.
						}

						// Should not be worse than what we could provide.
						if targetStart > windowProvideStart && targetEnd <= windowProvideEnd {
							log.Panic().Msg("T[" + utils.F("%02d", tidx) + "] " + "Unexpected neighbour window: [" + utils.V(targetStart) + ", " + utils.V(targetEnd) + ") " +
								", vs our window: [" + utils.V(prop.Windows[wIdx].Start) + ", " + utils.V(prop.Windows[wIdx].End) + ") " +
								" with edge: [" + utils.V(eS) + ", " + utils.V(eE) + ")\n" +
								" Our   windows: " + utils.V(prop.Windows) + "\n" +
								" Their windows: " + utils.V(targetWindows))
						}
					}
				}
			}

			// Check that the window is not a subset of any other window.
			for wIdx := range prop.Windows {
				for wIdx2 := range prop.Windows {
					if wIdx == wIdx2 {
						continue
					}
					if prop.Windows[wIdx].Start >= prop.Windows[wIdx2].Start && prop.Windows[wIdx].End <= prop.Windows[wIdx2].End {
						log.Panic().Msg("T[" + utils.F("%02d", tidx) + "] " + "Window " + utils.V(wIdx) + " is a subset of window " + utils.V(wIdx2) + " for vertex " + utils.V(gt.VertexRawID(i)) + " with windows: " + utils.V(prop.Windows))
					}
				}
			}

			if EXTEND_TO_EDGE_END {
				// Check if a window could have been merged into another window.
				for wIdx := range prop.Windows {
					for wIdx2 := range prop.Windows {
						if wIdx == wIdx2 {
							continue
						}
						if prop.Windows[wIdx].Start >= prop.Windows[wIdx2].Start && prop.Windows[wIdx].Start < prop.Windows[wIdx2].End {
							log.Panic().Msg("T[" + utils.F("%02d", tidx) + "] " + "Window " + utils.V(wIdx) + " could have been merged into window " + utils.V(wIdx2) + " for vertex " + utils.V(gt.VertexRawID(i)) + " with windows: " + utils.V(prop.Windows))
						}
					}
				}
			}
		}
		return 0
	})

	if COMPARE_SINGLE_TRAVERSAL {
		if !EXTEND_TO_EDGE_END {
			BasicTraversalMin(g)
		}
	}
}

// Debug: do a basic, single threaded algorithm as a traversal from the source.
func BasicTraversalMin(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	var startRaw graph.RawType
	for s := range g.InitMails {
		startRaw = s
		break
	}
	startInternal, _ := g.NodeVertexFromRaw(startRaw)
	reachableRanges := map[uint32][]Path{}
	reachableRanges[startInternal] = []Path{{Start: 0, End: math.MaxUint64, Hops: 0}}

	frontierList := []uint32{startInternal}
	nextList := []uint32{}
	nextUnique := map[uint32]bool{}

	for len(frontierList) > 0 {
		for _, vid := range frontierList {
			myRanges := reachableRanges[vid]
			for _, edge := range g.NodeVertex(vid).OutEdges {
			rangeLoop:
				for _, myRange := range myRanges {
					eS := edge.Property.GetTimestamp()
					eE := edge.Property.GetEndTime()
					if eE == 0 {
						eE = math.MaxUint64
					}
					if eS >= myRange.End {
						continue // Not a valid path: the edge started after/at the window end.
					}

					if eS <= myRange.Start {
						continue // Not a valid path: the edge started before/at the window start.
					}

					if eE <= myRange.Start {
						log.Panic().Msg("impossible curr " + utils.V(myRange) + " with edge end " + utils.V(eE))
						continue // Not possible to traverse (edge ends before/as my range starts)
					}
					offerStart := eS
					offerEnd := utils.Min(eE, myRange.End)
					if offerStart >= offerEnd {
						log.Panic().Msg("impossible")
						continue // No offer
					}

					// Look at their ranges.
					targetRanges := reachableRanges[edge.Didx]
					// sort target ranges by start
					sort.Slice(targetRanges, func(i, j int) bool {
						return targetRanges[i].Start < targetRanges[j].Start
					})

					// First try to extend a range.
					extended := false
					for tr := range targetRanges {
						// Check for subset paths
						if offerStart == targetRanges[tr].Start && offerEnd == targetRanges[tr].End {
							// Same range. Take min hops.
							if targetRanges[tr].Hops > myRange.Hops+1 {
								targetRanges[tr].Hops = myRange.Hops + 1
								continue rangeLoop
							}
							// no change
							continue rangeLoop
						} else if offerStart >= targetRanges[tr].Start && offerStart <= targetRanges[tr].End && offerEnd <= targetRanges[tr].End {
							// We offer a subset of this range.
							continue rangeLoop
						} else if offerStart <= targetRanges[tr].Start && offerEnd >= targetRanges[tr].End {
							// We offer a superset of this range.
							targetRanges[tr].Start = offerStart
							targetRanges[tr].End = offerEnd
							targetRanges[tr].Hops = myRange.Hops + 1
							extended = true
							break
						}
					}

					// If we didn't extend, add a new range.
					if !extended {
						targetRanges = append(targetRanges, Path{offerStart, offerEnd, myRange.Hops + 1})
					}
					if extended {
						// Merge overlapping ranges.
						for tr := 0; tr < len(targetRanges)-1; tr++ {
							if targetRanges[tr+1].Start <= targetRanges[tr].Start && targetRanges[tr+1].End >= targetRanges[tr].End {
								// Next (tr+1) is complete superset of current.
								targetRanges[tr].Start = targetRanges[tr+1].Start
								targetRanges[tr].End = targetRanges[tr+1].End
								targetRanges[tr].Hops = targetRanges[tr+1].Hops
								if tr+1 < len(targetRanges)-1 {
									copy(targetRanges[tr+1:], targetRanges[tr+2:])
								}
								targetRanges = targetRanges[:len(targetRanges)-1]
								tr--
							} else if targetRanges[tr+1].Start <= targetRanges[tr].End && targetRanges[tr+1].End <= targetRanges[tr].End {
								// Next (tr+1) is complete subset of current.
								if tr+1 < len(targetRanges)-1 {
									copy(targetRanges[tr+1:], targetRanges[tr+2:])
								}
								targetRanges = targetRanges[:len(targetRanges)-1]
								tr--
							}
						}
					}

					// Update their ranges, and add to the fronter to be updated.
					reachableRanges[edge.Didx] = targetRanges
					if in, ok := nextUnique[edge.Didx]; !in || !ok {
						nextUnique[edge.Didx] = true
						nextList = append(nextList, edge.Didx)
					}
				}
			}
		}

		// Swap the lists.
		frontierList, nextList = nextList, frontierList

		for k := range nextUnique {
			nextUnique[k] = false
		}
		nextList = nextList[:0]
	}

	/*
		// For some manual viewing...
		v := g.NodeVertex(startInternal)
		log.Info().Msg("Init (" + utils.V(startInternal) + ") " +
			"\nhas windows: " + utils.V(reachableRanges[startInternal]) +
			"\ngiven:       " + utils.V(v.Property.Windows))

		for e := 0; e < len(v.OutEdges); e++ {
			id, tidx := graph.InternalExpand(v.OutEdges[e].Didx)
			target := g.NodeVertex(v.OutEdges[e].Didx)
			if len(target.Property.Windows) != len(reachableRanges[v.OutEdges[e].Didx]) {
				log.Panic().Msg("\nT[" + utils.F("%02d", tidx) + "] " + utils.V(id) + " reachable vertex: " +
					"\n\nhas windows: " + utils.V(reachableRanges[v.OutEdges[e].Didx]) +
					"\n\ngiven:       " + utils.V(target.Property.Windows))
			}
		}
		time.Sleep(2 * time.Second)
	*/

	// Compare to results.
	for t := uint32(0); t < g.NumThreads; t++ {
		for i := uint32(0); i < uint32(len(g.GraphThreads[t].Vertices)); i++ {
			//vertex := &g.GraphThreads[t].Vertices[i]
			prop := g.GraphThreads[t].VertexProperty(i)
			internalId := ((t << graph.THREAD_SHIFT) | i)
			if _, in := reachableRanges[internalId]; !in {
				if len(prop.Windows) != 0 {
					log.Panic().Msg("\nT[" + utils.F("%02d", t) + "] od " + utils.V(i) + " Unreachable vertex given: " + utils.V(prop.Windows))
				}
				// else {
				//	log.Info().Msg("T[" + utils.F("%02d", t) + "] " + utils.V(i) + " Unreachable vertex has no windows.")
				//}
			}
			if len(prop.Windows) != len(reachableRanges[internalId]) {
				log.Panic().Msg("\nT[" + utils.F("%02d", t) + "] " + utils.V(i) + " reachable vertex: " +
					"\n\nhas windows: " + utils.V(reachableRanges[internalId]) +
					"\n\ngiven:       " + utils.V(prop.Windows))
			}
			// else {
			//	log.Info().Msg("T[" + utils.F("%02d", t) + "] " + utils.V(i) + " Reachable vertex has windows: " + utils.V(rR) + " compared to: " + utils.V(prop.Windows))
			//}
		}
	}
}
