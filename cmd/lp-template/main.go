package main

import (
	"github.com/ScottSallinen/lollipop/graph"
)

// Optional to declare.
// This function is mostly optional, but is a good way to describe
// an algorithm that can check for correctness of a result (outside just the go tests)
// For example, for breadth first search, you wouldn't expect a neighbour to be more than
// one hop away; for graph colouring you wouldn't expect two neighbours to have the same colour.
// This can codify the desire to ensure correct behaviour.
func (*Template) OnCheckCorrectness(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note]) {

}

// Optional to declare.
// Compares the results of the algorithm a view of the graph that is considered an oracle (has been run to complete convergence).
// Example implementation below (checks for difference vertex properties).
// Note this is more useful for algorithms that are somewhat approximate in nature; otherwise correctness checks (above)
// for a deterministic algorithm with an exact outcome is probably more useful.
func (*Template) OnOracleCompare(g *graph.Graph[VertexProperty, EdgeProperty, Message, Note], oracle *graph.Graph[VertexProperty, EdgeProperty, Message, Note]) {
	// This can be used as a simple compare of vertex properties. In this case gives a generic comparison of just a single vertex value.
	graph.OracleGenericCompareValues(g, oracle, func(vp VertexProperty) float64 { return vp.Value })
}

// Launch point. Parses command line arguments, and launches the graph execution.
func main() {
	// Define your own flags specific to the algorithm here, first. Example:
	// 	sourceInit := flag.String("i", "1", "Source init vertex (raw id).")
	graphOptions := graph.FlagsToOptions()

	// Some potential extra defines here, e.g. if the algorithm has a "point" initialization
	// or is instead initialized by default behaviour (where every vertex is visited initially).
	var initMessages map[graph.RawType]Message // Default is nil.
	// Example: send a message of 1 to vertex with the given raw ID, rather than starting with messaging all vertices.
	// initMessages[graph.AsRawTypeString(*sourceInit)] = 1.0

	graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Message, Note](new(Template), graphOptions, initMessages)
}
