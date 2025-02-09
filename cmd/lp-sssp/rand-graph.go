package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/path"
	"gonum.org/v1/gonum/graph/simple"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const (
	DEL_EDGE = 30
	ADD_EDGE = 100
)

type ShortestPathReport struct {
	Timestamp   int                `json:"timestamp"`
	DistanceMap map[uint32]float64 `json:"distance_map"`
}

func (p ShortestPathReport) MarshalJSON() ([]byte, error) {
	// Convert map[int]float64 to map[string]float64
	stringScores := make(map[string]float64)
	for k, v := range p.DistanceMap {
		stringScores[strconv.Itoa(int(k))] = v // Convert int key to string
	}

	// Create an alias to avoid recursion
	type Alias ShortestPathReport
	return json.Marshal(&struct {
		DistanceMap map[string]float64 `json:"distance_map"`
		*Alias
	}{
		DistanceMap: stringScores,
		Alias:       (*Alias)(&p),
	})
}

func writeToJson(filename string, reports []ShortestPathReport) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Convert struct to JSON
	jsonData, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return
	}

	_, err = file.Write(jsonData)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

func writeToFile(path string, lines []string) {
	// Open the file for writing
	file, err := os.Create(path) // Creates or truncates the file
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Create a buffered writer
	writer := bufio.NewWriter(file)

	// Write each line
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}

	// Ensure everything is written
	writer.Flush()

	fmt.Println("File written successfully!")
}

// GenerateRandomGraph creates a random graph with n nodes and m edges.
func GenerateRandomGraph(n, m int) ([]string, []ShortestPathReport) {
	g := simple.NewWeightedDirectedGraph(0, 0)

	// Create nodes
	nodes := make(map[int64]graph.Node)
	for i := 0; i < n; i++ {
		node, _ := g.NodeWithID(int64(i))
		g.AddNode(node)
		nodes[node.ID()] = node
	}

	var commands []string
	var reports []ShortestPathReport
	// Create random edges
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < m; {
		event := rand.Intn(ADD_EDGE + DEL_EDGE)
		if event >= DEL_EDGE && g.WeightedEdges().Len() < n*(n-1) { // Add edge
			var srcID, dstID int64
			for srcID, dstID = int64(rand.Intn(n)), int64(rand.Intn(n)); srcID == dstID || g.HasEdgeFromTo(srcID, dstID); srcID, dstID = int64(rand.Intn(n)), int64(rand.Intn(n)) {
			}
			weight := 1.0 // rand.Float64() * 10 // Random weight between 0 and 10
			g.SetWeightedEdge(g.NewWeightedEdge(nodes[srcID], nodes[dstID], weight))
			fmt.Printf("Added Edge between %v %v\n", srcID, dstID)
			commands = append(commands, fmt.Sprintf("%v %v", srcID, dstID))
		} else if g.WeightedEdges().Len() != 0 {
			edgeID := rand.Intn(g.WeightedEdges().Len())
			edgeIter := g.WeightedEdges()
			var edge graph.WeightedEdge
			for i := 0; i <= edgeID && edgeIter.Next(); i++ {
				edge = edgeIter.WeightedEdge()
			}
			g.RemoveEdge(edge.From().ID(), edge.To().ID())
			fmt.Printf("Deleted Edge between %v %v\n", edge.From().ID(), edge.To().ID())
			commands = append(commands, fmt.Sprintf("D %v %v", edge.From().ID(), edge.To().ID()))
		} else {
			continue
		}

		shortestFrom1 := path.DijkstraFrom(nodes[1], g)
		shortestMap := make(map[uint32]float64)
		for dstId := 0; dstId < n; dstId++ {
			shortestMap[uint32(dstId)] = min(math.MaxFloat64, shortestFrom1.WeightTo(int64(dstId)))
		}

		shortestPathReport := ShortestPathReport{
			Timestamp:   i,
			DistanceMap: shortestMap,
		}
		reports = append(reports, shortestPathReport)

		i++
	}

	//writeToFile(outputPath, commands)
	//writeToJson(jsonPath, reports)

	return commands, reports
}

// PrintGraph prints the nodes and edges of the graph.
func PrintGraph(g *simple.WeightedDirectedGraph) {
	fmt.Println("Graph Edges:")
	edgeIter := g.WeightedEdges()
	var edge graph.WeightedEdge
	for edgeIter.Next() {
		edge = edgeIter.WeightedEdge()
		fmt.Println(edge)
	}
}
