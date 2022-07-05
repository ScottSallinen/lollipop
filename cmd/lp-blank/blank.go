package main

import (
	"github.com/ScottSallinen/lollipop/graph"
)

func OnInitVertex(g *graph.Graph, vidx uint32, data interface{}) {
	g.Vertices[vidx].Properties.Value = 0.0
}

func OnEdgeAdd(g *graph.Graph, sidx uint32, didx uint32, data interface{}) {

}

func OnEdgeDel(g *graph.Graph, sidx uint32, didx uint32, data interface{}) {

}

func OnVisitVertex(g *graph.Graph, vidx uint32, data interface{}) int {

	return 0
}

func OnFinish(g *graph.Graph, data interface{}) error {

	return nil
}
