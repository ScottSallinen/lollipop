package main

import (
	"encoding/json"
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"io"
	"os"
	"strconv"
)

type ShortestPathReport struct {
	Timestamp   int                `json:"timestamp"`
	DistanceMap map[uint32]float64 `json:"distance_map"`
}

func (p ShortestPathReport) MarshalJSON() ([]byte, error) {
	// Convert map[int]float64 to map[string]float64
	stringDists := make(map[string]float64)
	for k, v := range p.DistanceMap {
		stringDists[strconv.Itoa(int(k))] = v // Convert int key to string
	}

	// Create an alias to avoid recursion
	type Alias ShortestPathReport
	return json.Marshal(&struct {
		DistanceMap map[string]float64 `json:"distance_map"`
		*Alias
	}{
		DistanceMap: stringDists,
		Alias:       (*Alias)(&p),
	})
}

func appendToJson(filename string, newReport ShortestPathReport) {
	// Step 1: Read the existing JSON file
	var reports []ShortestPathReport
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("File does not exist, creating a new one...")
			reports = []ShortestPathReport{} // Start with an empty list
		} else {
			fmt.Println("Error opening file:", err)
			return
		}
	} else {

		// Read the file content
		bytes, err := io.ReadAll(file)
		if err != nil {
			fmt.Println("Error reading file:", err)
			return
		}

		// Decode JSON (only if file is not empty)
		if len(bytes) > 0 {
			err = json.Unmarshal(bytes, &reports)
			if err != nil {
				fmt.Println("Error parsing JSON:", err)
				return
			}
		}

		file.Close()
	}

	file, err = os.Create(filename)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	reports = append(reports, newReport)

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

func (*SSSP) OnApplyTimeSeries(tse graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note]) {
	ssspReport := make(map[uint32]float64)
	tse.GraphView.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
		fmt.Println(tse.GraphView.NodeVertexRawID(v), prop.Value)
		ssspReport[tse.GraphView.NodeVertexRawID(v).Integer()] = prop.Value
	})
	appendToJson("cmd/rand-graph/rand-graph-sssp-actual.json",
		ShortestPathReport{
			int(tse.AtEventIndex),
			ssspReport,
		})
}
