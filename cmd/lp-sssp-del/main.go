package main

import (
	"flag"
	"github.com/rs/zerolog/log"
	"os"
	"sort"
	"sync"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Performs some sanity checks for correctness.
func (*SSSP) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	log.Debug().Msg("Checking correctness.")
	maxValue := make([]float64, g.NumThreads)

	distanceCountMap := map[float64]uint32{}
	lock := sync.RWMutex{}

	// Denote vertices that claim unvisited, and ensure out edges are at least as good as we could provide.
	visited := g.NodeParallelFor(func(_, threadOffset uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		tidx := gt.Tidx
		visitCount := 0
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vertex := &gt.Vertices[i]
			ourProp := gt.VertexProperty(i)
			ourValue := ourProp.Distance
			if ourValue < EmptyVal {
				maxValue[tidx] = utils.Max(maxValue[tidx], (ourValue))
				visitCount++
			}
			lock.Lock()
			if curr, ok := distanceCountMap[ourValue]; ok {
				distanceCountMap[ourValue] = curr + 1
			} else {
				distanceCountMap[ourValue] = 1
			}
			lock.Unlock()

			if ourValue == EmptyVal {
				// we were never visited.
			} else {
				for eidx := range vertex.OutEdges {
					targetProp := g.NodeVertexProperty(vertex.OutEdges[eidx].Didx)
					targetDistance := targetProp.Distance
					// Should not be worse than what we could provide.
					if targetDistance > (ourValue + vertex.OutEdges[eidx].Property.Weight) {
						incomingStr := "["
						for pred, v := range targetProp.IncomingVertices {
							if !v {
								log.Warn().Msg("Incoming vertex " + g.NodeVertexRawID(pred).String() + " of " + g.NodeVertexRawID(vertex.OutEdges[eidx].Didx).String() + " does not have us as a predecessor.")
							}
							incomingStr += g.NodeVertexRawID(pred).String() + ","
						}
						incomingStr += "]"
						notifHistory := "["
						for _, notif := range targetProp.InboxHistory {
							notifHistory += "{" + notif.Type.toString() + " From " + g.NodeVertexRawID(notif.Sender).String() + " at " + utils.V(notif.Ts) + "},"
						}
						notifHistory += "]"
						outboxHistory := "["
						for _, notif := range ourProp.OutboxHistory {
							outboxHistory += "{" + notif.Note.Type.toString() + " To " + g.NodeVertexRawID(notif.Target).String() + " at " + utils.V(notif.Note.Ts) + "},"
						}
						if targetProp.PredecessorVertex == EmptyVertex {
							log.Panic().Msg("Incorrect Distance(" + g.NodeVertexRawID(vertex.OutEdges[eidx].Didx).String() + ":" + utils.V(targetDistance) + " from EmptyVertex) (Incoming: " + incomingStr + ") = Shorter path from " + g.NodeVertexRawID(threadOffset|i).String() + "(" + utils.V(ourValue) + ")--w=" + utils.V(vertex.OutEdges[eidx].Property.Weight) + "-->" + g.NodeVertexRawID(vertex.OutEdges[eidx].Didx).String() + " Edge timestamp: " + utils.V(vertex.OutEdges[eidx].Property.Ts) + " LastNotif: " + notifHistory + " - Outbox: " + outboxHistory)
						} else {
							log.Panic().Msg("Incorrect Distance(" + g.NodeVertexRawID(vertex.OutEdges[eidx].Didx).String() + ":" + utils.V(targetDistance) + " from " + g.NodeVertexRawID(targetProp.PredecessorVertex).String() + ") (Incoming: " + incomingStr + ") = Shorter path from " + g.NodeVertexRawID(threadOffset|i).String() + "(" + utils.V(ourValue) + ")--w=" + utils.V(vertex.OutEdges[eidx].Property.Weight) + "-->" + g.NodeVertexRawID(vertex.OutEdges[eidx].Didx).String() + " Edge timestamp: " + utils.V(vertex.OutEdges[eidx].Property.Ts) + " LastNotif: " + notifHistory + " - Outbox: " + outboxHistory)
						}
					}
				}
			}
		}
		return visitCount
	})
	log.Info().Msg("Visited: " + utils.V(visited) + ", Percent: " + utils.F("%.3f", float64(visited)/float64(g.NodeVertexCount())*100.0))
	log.Info().Msg("MaxValue (longest shortest path): " + utils.V(utils.MaxSlice(maxValue)))
	var keys []float64
	for k, _ := range distanceCountMap {
		keys = append(keys, k)
	}
	sort.Float64s(keys)
	for _, key := range keys {
		value, _ := distanceCountMap[key]
		log.Info().Msg("Distance: " + utils.V(key) + ", Count: " + utils.V(value))
	}
}

// Compares the results of the algorithm to the oracle.
func (*SSSP) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	// Default compare function is fine; diffs should all be zero (algorithm is deterministic).
	log.Info().Msg("Comparing to oracle.")
	graph.OracleGenericCompareValues(g, oracle, func(vp VertexProperty) float64 { return vp.Distance })

}

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	_ = os.Remove("/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/actual_output.json")
	random := true
	if random {
		testSSSP()
		//V, E := 50, 500
		//testRandom(V, E, 1, 0.7, "/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/test_input.txt", "/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/expected_output.json", "/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/actual_output.json")
	} else {
		sourceInit := flag.String("i", "1", "Source init vertex (raw id).")
		graphOptions := graph.FlagsToOptions()
		//graphOptions.DebugLevel = 1
		alg, g := Run(graphOptions, sourceInit)
		graph.Launch(alg, g)
	}
}

//1_470_952_770 - 1_459_367_553
