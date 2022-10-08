package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

type VertexProperty struct {
	MergedEdges map[uint32]uint32
}
type EdgeProperty uint32 // Capacity
type MessageValue struct{}

func info(args ...interface{}) {
	log.Println("[Merger]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 2 || sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])

	capacity := 1 // Default capacity is 1
	if sflen == 3 {
		capacity, _ = strconv.Atoi(stringFields[2])
	}

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty(capacity)}
}

// Merge edges in a multi-graph
func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()

	graph.THREADS = *tptr

	g := &graph.Graph[VertexProperty, EdgeProperty, MessageValue]{}
	g.Options = graph.GraphOptions[MessageValue]{
		Undirected: false,
	}

	g.LoadGraphStatic(*gptr, EdgeParser)

	// Merge edges
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		v.Property.MergedEdges = make(map[uint32]uint32)
		for ei := range v.OutEdges {
			dst := v.OutEdges[ei].Destination
			v.Property.MergedEdges[dst] += uint32(v.OutEdges[ei].Property)
		}
	}

	mergedEdgeCount := uint32(0)
	minCapacity := uint32(math.MaxUint32)
	maxCapacity := uint32(0)
	totalCapacity := uint32(0)

	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		mergedEdgeCount += uint32(len(v.Property.MergedEdges))
		for _, capacity := range v.Property.MergedEdges {
			minCapacity = mathutils.Min(minCapacity, capacity)
			maxCapacity = mathutils.Max(maxCapacity, capacity)
			totalCapacity += capacity
		}
	}

	averageCapacity := float64(totalCapacity) / float64(mergedEdgeCount)

	info("Number of vertices: ", len(g.Vertices))
	info("Number of edges after merging: ", mergedEdgeCount)
	info("Average capacity: ", averageCapacity)
	info("Total capacity: ", totalCapacity)
	info("Maximum capacity: ", maxCapacity)
	info("Minimum capacity: ", minCapacity)

	f, err := os.Create(*gptr + ".merged")
	enforce.ENFORCE(err)
	defer func(f *os.File) {
		enforce.ENFORCE(f.Close())
	}(f)

	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		mergedEdgeCount += uint32(len(v.Property.MergedEdges))
		for dst, capacity := range v.Property.MergedEdges {
			_, err := f.WriteString(fmt.Sprintf("%d %d %d\n", v.Id, g.Vertices[dst].Id, capacity))
			enforce.ENFORCE(err)
		}
	}
}
