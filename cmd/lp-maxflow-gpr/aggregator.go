package main

import (
	"github.com/ScottSallinen/lollipop/graph"
)

type AggregatedMessage struct {
	FillEdges      bool
	skipIncreasing map[uint32]bool
}

func aggregateOnIncreasing(aggregated *AggregatedMessage, g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx, source, height uint32) {
	v := &g.Vertices[vidx]
	if v.Property.Type == Sink {
		return
	}

	neighbour := v.Property.Neighbours[source]

	v.Property.Neighbours[source] = Neighbour{
		Height:           height,
		ResidualCapacity: neighbour.ResidualCapacity,
	}

	if neighbour.ResidualCapacity == 0 {
		// No flow can be pushed to this neighbour
		return
	}
	aggregated.FillEdges = true
}

func ProcessAggregatedMessage(aggregated *AggregatedMessage, g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32) (messageCount int) {
	if aggregated.FillEdges {
		messageCount += fillNeighbours(g, vidx, aggregated.skipIncreasing)
	}
	return messageCount
}

func MaxFlowMessageAggregator(g *graph.Graph[VertexProperty, EdgeProperty, MessageValue], vidx uint32, VisitMsg MessageValue) *AggregatedMessage {
	//v := &g.Vertices[vidx]
	aggregated := AggregatedMessage{}
	aggregated.skipIncreasing = make(map[uint32]bool)
	for messageIndex := range VisitMsg {
		m := &VisitMsg[messageIndex]
		switch m.Type {
		case Increasing:
			aggregated.skipIncreasing[m.Source] = true
			aggregateOnIncreasing(&aggregated, g, vidx, m.Source, m.Height)
		}
	}
	return &aggregated
}
