package main

import (
	"flag"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Performs some sanity checks for correctness.
func (*SSSP) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	maxValue := make([]float64, g.NumThreads)
	numDistZero := uint64(0)
	numDistOne := uint64(0)
	numDistTwo := uint64(0)
	numDistThree := uint64(0)
	numDistFour := uint64(0)

	// Denote vertices that claim unvisited, and ensure out edges are at least as good as we could provide.
	visited := g.NodeParallelFor(func(_, _ uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		tidx := gt.Tidx
		visitCount := 0
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vertex := &gt.Vertices[i]
			ourValue := vertex.Property.Value
			if ourValue < EMPTY_VAL {
				maxValue[tidx] = utils.Max(maxValue[tidx], (ourValue))
				visitCount++
			}
			if ourValue == 0 {
				atomic.AddUint64(&numDistZero, 1)
			} else if ourValue == 1 {
				atomic.AddUint64(&numDistOne, 1)
			} else if ourValue == 2 {
				atomic.AddUint64(&numDistTwo, 1)
			} else if ourValue == 3 {
				atomic.AddUint64(&numDistThree, 1)
			} else if ourValue == 4 {
				atomic.AddUint64(&numDistFour, 1)
			}

			if initVal, ok := g.InitMails[gt.VertexRawID(i)]; ok {
				if ourValue != float64(initVal) {
					log.Panic().Msg("Expected rawId " + utils.V(gt.VertexRawID(i)) + " to have init, but has " + utils.V(ourValue))
				}
			}
			if ourValue == EMPTY_VAL {
				// we were never visited.
			} else {
				for eidx := range vertex.OutEdges {
					targetProp := g.NodeVertex(vertex.OutEdges[eidx].Didx).Property.Value
					// Should not be worse than what we could provide.
					if targetProp > (ourValue + vertex.OutEdges[eidx].Property.Weight) {
						log.Panic().Msg("Unexpected neighbour weight: " + utils.V(targetProp) + ", vs our weight: " + utils.V(ourValue) + " with edge weight: " + utils.V(vertex.OutEdges[eidx].Property.Weight))
					}
				}
			}
		}
		return visitCount
	})
	log.Info().Msg("Visited: " + utils.V(visited) + ", Percent: " + utils.F("%.3f", float64(visited)/float64(g.NodeVertexCount())*100.0))
	log.Info().Msg("MaxValue (longest shortest path): " + utils.V(utils.MaxSlice(maxValue)))
	log.Info().Msg("Num with distances of: 0: " + utils.V(numDistZero) + ", 1: " + utils.V(numDistOne) + ", 2: " + utils.V(numDistTwo) + ", 3: " + utils.V(numDistThree) + ", 4: " + utils.V(numDistFour))
}

// Compares the results of the algorithm to the oracle.
func (*SSSP) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	// Default compare function is fine; diffs should all be zero (algorithm is deterministic).
	graph.OracleGenericCompareValues(g, oracle, func(vp VertexProperty) float64 { return vp.Value })
}

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	sourceInit := flag.String("i", "1", "Source init vertex (raw id).")
	useMsgPassing := flag.Bool("msg", false, "Use message passing. This is slow! Only for a reference implementation of message passing.")
	graphOptions := graph.FlagsToOptions()

	if !(*useMsgPassing) {
		initMail := map[graph.RawType]Mail{}
		initMail[graph.AsRawTypeString(*sourceInit)] = 0.0
		graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(SSSP), graphOptions, initMail, nil)
	} else {
		log.Warn().Msg("Warning: this strategy is slow! Use this only for reference.")
		initNotes := map[graph.RawType]NoteMsg{}
		initNotes[graph.AsRawTypeString(*sourceInit)] = 0.0
		graph.LaunchGraphExecution[*EPMsg, VPMsg, EPMsg, MailMsg, NoteMsg](new(SSSPM), graphOptions, nil, initNotes)
	}
}
