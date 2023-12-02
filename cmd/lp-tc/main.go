package main

import (
	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/utils"
)

// Performs some checks for correctness.
func (*TC) OnCheckCorrectness(g *graph.Graph[VertexProp, EdgeProp, Mail, Note]) {
	// TotalMapChecks := uint64(0)
	TotalTriangles := g.NodeParallelFor(func(_, _ uint32, gt *graph.GraphThread[VertexProp, EdgeProp, Mail, Note]) int {
		ourTriangles := uint32(0)
		// ourMapChecks := uint64(0)
		for i := uint32(0); i < uint32(len(gt.Vertices)); i++ {
			ourTriangles += gt.VertexProperty(i).NumLeader
			// ourMapChecks += gt.VertexProperty(i).MapChecks
			// The vertex should have advanced their update position to the end of their edges.
			if int(gt.VertexProperty(i).UpdateAt) != len(gt.Vertices[i].OutEdges) {
				log.Panic().Msg("Vertex " + utils.V(gt.VertexRawID(i)) + " has update pos at " + utils.V(gt.VertexProperty(i).UpdateAt) + " but edge len is " + utils.V(len(gt.Vertices[i].OutEdges)))
			}
		}
		// atomic.AddUint64(&TotalMapChecks, ourMapChecks)
		return int(ourTriangles)
	})
	log.Info().Msg("Number of triangles: " + utils.V(TotalTriangles)) // + " Total map checks: " + utils.V(TotalMapChecks))
}

// Compares the results of the algorithm to the oracle.
func (*TC) OnOracleCompare(g *graph.Graph[VertexProp, EdgeProp, Mail, Note], oracle *graph.Graph[VertexProp, EdgeProp, Mail, Note]) {
	// Default compare function is fine; diffs should all be zero (algorithm is deterministic).
	graph.OracleGenericCompareValues(g, oracle, func(vp VertexProp) uint32 { return (vp.NumLeader) })
}

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	graphOptions := graph.FlagsToOptions()
	graphOptions.Undirected = true // This algorithm is intended for counting undirected triangles.

	graph.LaunchGraphExecution[*EdgeProp, VertexProp, EdgeProp, Mail, Note](new(TC), graphOptions, nil, nil)
}
