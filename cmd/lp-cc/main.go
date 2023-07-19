package main

import (
	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Performs some sanity checks for correctness.
func (*CC) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	uniqueComponents := make(map[uint32]bool)

	// Make sure the labels inside connected components are consistent
	g.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty]) {
		ourValue := vertex.Property.Value
		if ourValue == EMPTY_VAL {
			log.Panic().Msg("vertex " + utils.V(g.NodeVertexRawID(v)) + " is not labelled")
		}
		if _, ok := uniqueComponents[ourValue]; !ok {
			uniqueComponents[ourValue] = true
		}
		for eidx := range vertex.OutEdges {
			target := vertex.OutEdges[eidx].Didx
			if g.NodeVertex(target).Property.Value != ourValue {
				log.Panic().Msg("Connected vertex with different labels: " + utils.V(g.NodeVertex(target).Property.Value) + ", " + utils.V(ourValue))
			}
		}
	})
	log.Info().Msg("Number of unique components: " + utils.V(len(uniqueComponents)))
}

// Compares the results of the algorithm to the oracle.
func (*CC) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Mail, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Mail, Note]) {
	// Default compare function is fine; diffs should all be zero (algorithm is deterministic).
	graph.OracleGenericCompareValues(g, oracle, func(vp VertexProperty) uint32 { return vp.Value })
}

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	graphOptions := graph.FlagsToOptions()
	graphOptions.Undirected = true // undirected should always be true.
	graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(CC), graphOptions, nil, nil)
}
