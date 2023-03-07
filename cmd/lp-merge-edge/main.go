package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

type VertexProperty struct {
	MergedEdges map[uint32]EdgeProperty
}
type Edge struct {
	Src      uint32
	Dst      uint32
	Property EdgeProperty
}
type EdgeProperty struct {
	Capacity  uint32
	Timestamp uint64
}
type MessageValue struct{}

func info(args ...interface{}) {
	log.Println("[Merger]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)
	enforce.ENFORCE(len(stringFields) == 3)

	src, err := strconv.Atoi(stringFields[0])
	enforce.ENFORCE(err)
	dst, err := strconv.Atoi(stringFields[1])
	enforce.ENFORCE(err)
	timestamp, err := strconv.Atoi(stringFields[2])
	enforce.ENFORCE(err)

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{
		Capacity:  1,
		Timestamp: uint64(timestamp),
	}}
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

		mergedEdges := make(map[uint32]struct {
			Timestamps []uint64
			Capacity   uint32
		})
		for ei := range v.OutEdges {
			e := &v.OutEdges[ei]
			merged := mergedEdges[e.Destination]
			merged.Timestamps = append(merged.Timestamps, e.Property.Timestamp)
			merged.Capacity += e.Property.Capacity
			mergedEdges[e.Destination] = merged
		}

		v.Property.MergedEdges = make(map[uint32]EdgeProperty)
		for dst, edge := range mergedEdges {
			v.Property.MergedEdges[dst] = EdgeProperty{
				Capacity:  edge.Capacity,
				Timestamp: mathutils.Sum(edge.Timestamps) / uint64(len(edge.Timestamps)),
			}
		}
	}

	// Compute some statistics
	mergedEdgeCount := uint32(0)
	minCapacity := uint32(math.MaxUint32)
	maxCapacity := uint32(0)
	totalCapacity := uint32(0)
	capacities := make([]int, 0)

	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		mergedEdgeCount += uint32(len(v.Property.MergedEdges))
		for _, property := range v.Property.MergedEdges {
			minCapacity = mathutils.Min(minCapacity, property.Capacity)
			maxCapacity = mathutils.Max(maxCapacity, property.Capacity)
			totalCapacity += property.Capacity
			capacities = append(capacities, int(property.Capacity))
		}
	}

	averageCapacity := float64(totalCapacity) / float64(mergedEdgeCount)
	medianCapacity := mathutils.Median(capacities)

	info("Number of vertices: ", len(g.Vertices))
	info("Number of edges after merging: ", mergedEdgeCount)
	info("Average capacity: ", averageCapacity)
	info("Total capacity: ", totalCapacity)
	info("Maximum capacity: ", maxCapacity)
	info("Minimum capacity: ", minCapacity)
	info("Median capacity: ", medianCapacity)

	// Convert to a list
	mergedEdges := make([]Edge, 0, mergedEdgeCount) // 0-indexed
	for vi := range g.Vertices {
		v := &g.Vertices[vi]
		for dst, property := range v.Property.MergedEdges {
			mergedEdges = append(mergedEdges, Edge{
				Src:      g.VertexMap[v.Id],
				Dst:      dst,
				Property: property,
			})
		}
	}

	// Sort by timestamps
	sort.Slice(mergedEdges, func(i, j int) bool {
		ei := &mergedEdges[i]
		ej := &mergedEdges[j]
		if ei.Property.Timestamp < ej.Property.Timestamp {
			return true
		} else if ei.Property.Timestamp == ej.Property.Timestamp {
			if ei.Src < ej.Src {
				return true
			} else if ei.Src == ej.Src {
				if ei.Dst < ej.Dst {
					return true
				} else if ei.Dst == ej.Dst {
					return ei.Property.Capacity < ej.Property.Capacity
				}
			}
		}
		return false
	})

	mergedFile, err := os.Create(*gptr + ".merged")
	enforce.ENFORCE(err)
	mergedFileNoTimestamp, err := os.Create(*gptr + ".merged-notimestamp")
	enforce.ENFORCE(err)

	for i := range mergedEdges {
		e := &mergedEdges[i]
		_, err := mergedFile.WriteString(fmt.Sprintf("%d %d %d %d\n",
			e.Src, e.Dst, e.Property.Capacity, e.Property.Timestamp))
		enforce.ENFORCE(err)
		_, err = mergedFileNoTimestamp.WriteString(fmt.Sprintf("%d %d %d\n",
			e.Src, e.Dst, e.Property.Capacity))
		enforce.ENFORCE(err)
	}

	enforce.ENFORCE(mergedFile.Close())
	enforce.ENFORCE(mergedFileNoTimestamp.Close())
}
