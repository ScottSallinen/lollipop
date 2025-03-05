package main

import (
	"encoding/json"
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"io"
	"os"
	//"strconv"
)

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
		//fmt.Println(tse.GraphView.NodeVertexRawID(v), prop.Predecessor.TotalDistance)
		ssspReport[tse.GraphView.NodeVertexRawID(v).Integer()] = prop.Distance
	})
	appendToJson("/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/actual_output.json",
		ShortestPathReport{
			Timestamp:   int(tse.AtEventIndex),
			DistanceMap: ssspReport,
		})
}

//import (
//	"fmt"
//	"github.com/ScottSallinen/lollipop/graph"
//	//"strconv"
//)
//
//func (*SSSP) OnApplyTimeSeries(tse graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note]) {
//	tse.GraphView.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
//		fmt.Printf("Vertex %v - Pred %v - Distance: %v\n", tse.GraphView.NodeVertexRawID(v), prop.PredecessorVertex, prop.Distance)
//	})
//}
