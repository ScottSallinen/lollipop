package main

import (
	"github.com/ScottSallinen/lollipop/graph"
)

func MessageAggregator(target *graph.Vertex, data float64) (newInfo bool) {
	return false
}

func AggregateRetrieve(target *graph.Vertex) float64 {
	return 0.0
}

func OnInitVertex(g *graph.Graph, vidx uint32) {
	g.Vertices[vidx].Value = 0.0
}

func OnEdgeAdd(g *graph.Graph, sidx uint32, didx map[uint32]int, data float64) {

}

func OnEdgeDel(g *graph.Graph, sidx uint32, didx uint32, data float64) {

}

func OnVisitVertex(g *graph.Graph, vidx uint32, data float64) int {
	return 0
}

func OnFinish(g *graph.Graph) error {
	return nil
}
