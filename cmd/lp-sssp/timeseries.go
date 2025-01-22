package main

import (
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
)

func (*SSSP) OnApplyTimeSeries(tse graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note]) {
	tse.GraphView.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
		fmt.Println(tse.GraphView.NodeVertexRawID(v), prop.Value)
	})
}
