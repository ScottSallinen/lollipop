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
	distanceCountMap := sync.Map{}

	// Denote vertices that claim unvisited, and ensure out edges are at least as good as we could provide.
	visited := g.NodeParallelFor(func(_, threadOffset uint32, gt *graph.GraphThread[VertexProperty, EdgeProperty, Mail, Note]) int {
		tidx := gt.Tidx
		visitCount := 0
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			vertex := &gt.Vertices[i]
			ourValue := gt.VertexProperty(i).Distance
			if ourValue < EmptyVal {
				maxValue[tidx] = utils.Max(maxValue[tidx], (ourValue))
				visitCount++
			}
			if curr, ok := distanceCountMap.Load(ourValue); ok {
				distanceCountMap.Store(ourValue, curr.(int)+1)
			} else {
				distanceCountMap.Store(ourValue, 1)
			}

			if ourValue == EmptyVal {
				// we were never visited.
			} else {
				for eidx := range vertex.OutEdges {
					targetProp := g.NodeVertexProperty(vertex.OutEdges[eidx].Didx).Distance
					// Should not be worse than what we could provide.
					if targetProp > (ourValue + vertex.OutEdges[eidx].Property.Weight) {
						log.Warn().Msg("Unexpected neighbour From(" + g.NodeVertexRawID(threadOffset|i).String() + "->" + g.NodeVertexRawID(vertex.OutEdges[eidx].Didx).String() + ") weight: " + utils.V(targetProp) + ", vs our weight: " + utils.V(ourValue) + " with edge weight: " + utils.V(vertex.OutEdges[eidx].Property.Weight) + "Edge timestamp: " + utils.V(vertex.OutEdges[eidx].Property.Ts) + " Delete Mark: " + utils.V(vertex.OutEdges[eidx].Pos&(1<<31)))
					}
				}
			}
		}
		return visitCount
	})
	log.Info().Msg("Visited: " + utils.V(visited) + ", Percent: " + utils.F("%.3f", float64(visited)/float64(g.NodeVertexCount())*100.0))
	log.Info().Msg("MaxValue (longest shortest path): " + utils.V(utils.MaxSlice(maxValue)))
	var keys []float64
	distanceCountMap.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(float64))
		return true
	})
	sort.Float64s(keys)
	for _, key := range keys {
		value, _ := distanceCountMap.Load(key)
		log.Info().Msg("Distance: " + utils.V(key) + ", Count: " + utils.V(value))
	}
}

// Compares the results of the algorithm to the oracle.
func (*SSSP) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	// Default compare function is fine; diffs should all be zero (algorithm is deterministic).
	graph.OracleGenericCompareValues(g, oracle, func(vp VertexProperty) float64 { return vp.Distance })
}

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	_ = os.Remove("/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/actual_output.json")
	random := false
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
