package main

import (
	"encoding/json"
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/rs/zerolog/log"
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

var OutputFilename = "/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/tse_stats_del"

func (alg *SSSP) OnApplyTimeSeries(tse graph.TimeseriesEntry[VertexProperty, EdgeProperty, Mail, Note]) {
	alg.OnCheckCorrectness(tse.GraphView)

	_, err := os.Stat(OutputFilename)
	fileExisted := !os.IsNotExist(err)

	file, err := os.OpenFile(OutputFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error().Msg("Error opening/creating file:" + err.Error())
		return
	}
	defer file.Close()

	var line string
	if !fileExisted {
		line = fmt.Sprintf("Name,AtEventIndex,EdgeCount,EdgeDelete,Latency,AlgTimeSinceLast,CurrentRuntime\n")
		if _, err := file.WriteString(line); err != nil {
			log.Error().Msg("Error writing to file:" + err.Error())
		}
	}
	line = fmt.Sprintf("%d,%d,%d,%d,%d,%d,%d\n", tse.Name.Nanosecond(), tse.AtEventIndex, tse.EdgeCount, tse.EdgeDeletes, tse.Latency.Nanoseconds(), tse.AlgTimeSinceLast.Nanoseconds(), tse.CurrentRuntime.Nanoseconds())
	if _, err := file.WriteString(line); err != nil {
		log.Error().Msg("Error writing to file:" + err.Error())
	}

	//ssspReport := make(map[uint32]float64)
	//tse.GraphView.NodeForEachVertex(func(i, v uint32, vertex *graph.Vertex[VertexProperty, EdgeProperty], prop *VertexProperty) {
	//	//fmt.Println(tse.GraphView.NodeVertexRawID(v), prop.Predecessor.TotalDistance)
	//	ssspReport[tse.GraphView.NodeVertexRawID(v).Integer()] = prop.Distance
	//})
	//appendToJson("/Users/pjavanrood/Documents/NetSys/lollipop/cmd/lp-sssp-del/actual_output.json",
	//	ShortestPathReport{
	//		Timestamp:   int(tse.AtEventIndex),
	//		DistanceMap: ssspReport,
	//	})
}
