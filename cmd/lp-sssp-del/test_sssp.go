package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ScottSallinen/lollipop/graph"
	"io"
	"os"
)

type TestCase struct {
	Name          string               `json:"name"`
	Events        []string             `json:"events"`
	Expected      []ShortestPathReport `json:"expected"`
	QueryInterval int                  `json:"query_interval"`
}

func writeTestInput(filename string, events []string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, event := range events {
		_, err := file.WriteString(event + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func writeExpectedOutput(filename string, reports []ShortestPathReport) {
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

func readTestOutput(filename string) ([]ShortestPathReport, error) {
	var reports []ShortestPathReport
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return reports, err
	} else {
		defer file.Close()
		// Read the file content
		bytes, err := io.ReadAll(file)
		if err != nil {
			fmt.Println("Error reading file:", err)
			return reports, err
		}

		// Decode JSON (only if file is not empty)
		if len(bytes) > 0 {
			err = json.Unmarshal(bytes, &reports)
			if err != nil {
				fmt.Println("Error parsing JSON:", err)
				return reports, err
			}
		}
	}
	return reports, nil
}

func ssspAlgorithm(filename string) {
	// Placeholder for the actual SSSP algorithm implementation
	graphOptions := graph.FlagsToOptions()

	initMail := map[graph.RawType]Mail{}
	alg := new(SSSP)
	alg.SourceVertex = graph.AsRawType(1)
	//initMail[graph.AsRawTypeString("1")] = Mail{NewSafeMail()}
	graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](alg, graphOptions, initMail, nil)
}

func runTestCase(test TestCase, inputPath, expectedPath, actualPath string) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Set custom args
	os.Args = []string{"", "-g", inputPath, "-d", "-de", fmt.Sprintf("%v", test.QueryInterval), "-debug", "1"}

	// Write test input file
	err := writeTestInput(inputPath, test.Events)
	if err != nil {
		fmt.Println("Error writing test file:", err)
		return
	}

	_ = os.Remove(actualPath)

	// Run the SSSP algorithm
	ssspAlgorithm(inputPath)

	reports, err := readTestOutput(actualPath)
	// Load expected results
	if err != nil {
		fmt.Println("Error marshaling expected data:", err)
		return
	}

	writeExpectedOutput(expectedPath, test.Expected)

	// map timestamp to expected results
	mapTimestampExpectedResult := make(map[int]ShortestPathReport)
	for _, expected := range test.Expected {
		mapTimestampExpectedResult[expected.Timestamp] = expected
	}

	// map timestamp to actual results
	mapTimestampActualResult := make(map[int]ShortestPathReport)
	for _, actual := range reports {
		mapTimestampActualResult[actual.Timestamp] = actual
	}

	//for timestamp, expectedReport := range mapTimestampExpectedResult {
	//	actualReport := mapTimestampActualResult[timestamp]
	//	for v, edv := range expectedReport.DistanceMap {
	//		if dv, exist := actualReport.DistanceMap[v]; !exist {
	//			if edv == EmptyVal {
	//				continue
	//			}
	//			fmt.Println(fmt.Sprintf("Test [%v] Failed: timestamp %v vertex %v didn't exist!", test.Name, timestamp, v))
	//			//return
	//		} else if dv != edv {
	//			fmt.Println(fmt.Sprintf("Test [%v] Failed: timestamp %v vertex %v wrong distance [expected %v] != [got %v]!", test.Name, timestamp, v, edv, dv))
	//			//return
	//		}
	//	}
	//}

	//// Compare results
	for _, actualReport := range reports {
		timestamp := actualReport.Timestamp
		expectedReport := mapTimestampExpectedResult[timestamp]
		for v, edv := range expectedReport.DistanceMap {
			if dv, exist := actualReport.DistanceMap[v]; !exist {
				if edv == EmptyVal {
					continue
				}
				fmt.Println(fmt.Sprintf("Test [%v] Failed: timestamp %v vertex %v didn't exist!", test.Name, timestamp, v))
				//return
			} else if dv != edv {
				fmt.Println(fmt.Sprintf("Test [%v] Failed: timestamp %v vertex %v wrong distance [expected %v] != [got %v]!", test.Name, timestamp, v, edv, dv))
				//return
			}
		}
	}
	fmt.Println("Passed Test Case:", test.Name)

}

func testRandom(V, E, queryInterval int, addProb float64, inputPath, expectedPath, actualPath string) {
	commands, reports := GenerateRandomGraph(V, E, Probability(addProb), inputPath, expectedPath)
	randomTest := TestCase{
		Name:          fmt.Sprintf("random_%v_%v", V, E),
		Events:        commands,
		Expected:      reports,
		QueryInterval: queryInterval,
	}
	runTestCase(randomTest, inputPath, expectedPath, actualPath)
}

func testSSSP() {
	testCases := []TestCase{
		{
			Name: "basic_add_delete",
			Events: []string{
				"1 2",
				"2 3",
				"D 1 2",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EmptyVal}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: EmptyVal, 3: EmptyVal}},
			},
		},
		{
			Name: "disconnected_to_connected",
			Events: []string{
				"1 4",
				"4 5",
				"5 2",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 4: 1, 5: EmptyVal, 2: EmptyVal}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 4: 1, 5: 2, 2: EmptyVal}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 4: 1, 5: 2, 2: 3}},
			},
		},
		{
			Name: "graph_becomes_disconnected",
			Events: []string{
				"1 2",
				"2 3",
				"D 2 3",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EmptyVal}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EmptyVal}},
			},
		},
		{
			Name: "cycle_formation_breaking",
			Events: []string{
				"1 2",
				"2 3",
				"3 1",
				"D 3 1",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EmptyVal}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
			},
		},
		{
			Name: "path_shortening_and_lengthening",
			Events: []string{
				"1 2",
				"2 3",
				"1 3",
				"D 1 3",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EmptyVal}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 1}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
			},
		},
		{
			Name: "large_sparse_graph",
			Events: []string{
				"1 10",
				"10 20",
				"20 30",
				"30 40",
				"40 50",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: EmptyVal, 30: EmptyVal, 40: EmptyVal, 50: EmptyVal}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: EmptyVal, 40: EmptyVal, 50: EmptyVal}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: 3, 40: EmptyVal, 50: EmptyVal}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: 3, 40: 4, 50: EmptyVal}},
				{Timestamp: 4, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: 3, 40: 4, 50: 5}},
			},
		},
		{
			Name: "frequent_add_delete",
			Events: []string{
				"1 2",
				"D 1 2",
				"1 2",
				"D 1 2",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: EmptyVal}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 2: EmptyVal}},
			},
		},
		{
			Name: "cycle_add_delete",
			Events: []string{
				"1 2",
				"2 3",
				"3 4",
				"4 5",
				"5 2",
				"D 1 2",
				"5 3",
				"1 2",
				"D 2 3",
				"2 3",
			},
			Expected: []ShortestPathReport{
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}},
				{Timestamp: 4, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}},
				{Timestamp: 5, DistanceMap: map[uint32]float64{1: 0, 2: EmptyVal, 3: EmptyVal, 4: EmptyVal, 5: EmptyVal}},
				{Timestamp: 6, DistanceMap: map[uint32]float64{1: 0, 2: EmptyVal, 3: EmptyVal, 4: EmptyVal, 5: EmptyVal}},
				{Timestamp: 7, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}},
				{Timestamp: 8, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EmptyVal, 4: EmptyVal, 5: EmptyVal}},
				{Timestamp: 9, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}},
			},
		},
	}

	for _, test := range testCases {
		//break
		runTestCase(test, "", "", "")
	}
}
