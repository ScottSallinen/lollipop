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
	Name     string               `json:"name"`
	Events   []string             `json:"events"`
	Expected []ShortestPathReport `json:"expected"`
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
	initMail[graph.AsRawTypeString("1")] = Mail{NewConcurrentMap[uint32, Predecessor]()}
	graph.LaunchGraphExecution[*EdgeProperty, VertexProperty, EdgeProperty, Mail, Note](new(SSSP), graphOptions, initMail, nil)
}

func runTestCase(test TestCase, nQuery int, checkOutput bool) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	txtFilename := test.Name + ".txt"
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Set custom args
	if checkOutput {
		os.Args = []string{"", "-g", txtFilename, "-de", fmt.Sprintf("%v", nQuery)}
	} else {
		os.Args = []string{"", "-g", txtFilename}
	}

	// Write test input file
	err := writeTestInput(txtFilename, test.Events)
	if err != nil {
		fmt.Println("Error writing test file:", err)
		return
	}

	_ = os.Remove("cmd/rand-graph/rand-graph-sssp-actual.json")

	// Run the SSSP algorithm
	ssspAlgorithm(txtFilename)

	reports, err := readTestOutput("cmd/rand-graph/rand-graph-sssp-actual.json")
	// Load expected results
	if err != nil {
		fmt.Println("Error marshaling expected data:", err)
		return
	}

	writeExpectedOutput("cmd/rand-graph/rand-graph-sssp-expected.json", test.Expected)

	// map timestamp to expected results
	mapTimestampExpectedResult := make(map[int]ShortestPathReport)
	for _, expected := range test.Expected {
		mapTimestampExpectedResult[expected.Timestamp] = expected
	}

	if !checkOutput {
		return
	}

	// Compare results
	for _, actualReport := range reports {
		timestamp := actualReport.Timestamp
		expectedReport := mapTimestampExpectedResult[timestamp]
		for v, edv := range expectedReport.DistanceMap {
			if dv, exist := actualReport.DistanceMap[v]; !exist {
				if edv == EMPTY_VAL {
					continue
				}
				fmt.Println(fmt.Sprintf("Test [%v] Failed: timestamp %v vertex %v didn't exist!", test.Name, timestamp, v))
				return
			} else if dv != edv {
				fmt.Println(fmt.Sprintf("Test [%v] Failed: timestamp %v vertex %v wrong distance [expected %v] != [got %v]!", test.Name, timestamp, v, edv, dv))
				return
			}
		}
	}
	fmt.Println("Passed Test Case:", test.Name)

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
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EMPTY_VAL}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: EMPTY_VAL, 3: EMPTY_VAL}},
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
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 4: 1, 5: EMPTY_VAL, 2: EMPTY_VAL}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 4: 1, 5: 2, 2: EMPTY_VAL}},
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
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EMPTY_VAL}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EMPTY_VAL}},
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
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EMPTY_VAL}},
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
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EMPTY_VAL}},
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
				{Timestamp: 0, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: EMPTY_VAL, 30: EMPTY_VAL, 40: EMPTY_VAL, 50: EMPTY_VAL}},
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: EMPTY_VAL, 40: EMPTY_VAL, 50: EMPTY_VAL}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: 3, 40: EMPTY_VAL, 50: EMPTY_VAL}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 10: 1, 20: 2, 30: 3, 40: 4, 50: EMPTY_VAL}},
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
				{Timestamp: 1, DistanceMap: map[uint32]float64{1: 0, 2: EMPTY_VAL}},
				{Timestamp: 2, DistanceMap: map[uint32]float64{1: 0, 2: 1}},
				{Timestamp: 3, DistanceMap: map[uint32]float64{1: 0, 2: EMPTY_VAL}},
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
				{Timestamp: 5, DistanceMap: map[uint32]float64{1: 0, 2: EMPTY_VAL, 3: EMPTY_VAL, 4: EMPTY_VAL, 5: EMPTY_VAL}},
				{Timestamp: 6, DistanceMap: map[uint32]float64{1: 0, 2: EMPTY_VAL, 3: EMPTY_VAL, 4: EMPTY_VAL, 5: EMPTY_VAL}},
				{Timestamp: 7, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}},
				{Timestamp: 8, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: EMPTY_VAL, 4: EMPTY_VAL, 5: EMPTY_VAL}},
				{Timestamp: 9, DistanceMap: map[uint32]float64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4}},
			},
		},
	}

	for _, test := range testCases {
		break
		runTestCase(test, 1, true)
	}

	n := 100
	m := n * (n - 1)

	commands, reports := GenerateRandomGraph(n, m)
	randomTest := TestCase{
		Name:     fmt.Sprintf("random_%v_%v", n, m),
		Events:   commands,
		Expected: reports,
	}
	runTestCase(randomTest, m, false)
}
