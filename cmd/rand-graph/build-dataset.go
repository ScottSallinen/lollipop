package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

type Edge struct {
	timestamp  int
	sourceDest string
}

type MinHeap []Edge

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].timestamp < h[j].timestamp }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(Edge))
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[0]
	*h = old[1:n]
	return x
}

func (h *MinHeap) Peek() (Edge, bool) {
	if h.Len() == 0 {
		return Edge{}, false
	}
	return (*h)[0], true
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <input_file> <output_file>")
		return
	}

	inputFileName := os.Args[1]
	outputFileName := os.Args[2]
	deltaDays, _ := strconv.Atoi(os.Args[3])
	deltaSeconds := deltaDays * 24 * 60 * 60
	deleteProbability := 0.0
	if len(os.Args) > 4 {
		deleteProbability, _ = strconv.ParseFloat(os.Args[4], 64)
	}
	inputFile := ReadFile(inputFileName)

	mergedAdds, e := MergeAdds(inputFile, deltaSeconds, false)
	if e != nil {
		_ = fmt.Errorf("Error merging adds: %v\n", e)
		return
	}
	if deleteProbability == 0 {
		WriteToFile(outputFileName, mergedAdds)
		return
	}

	injectedDeletes, e := InjectDeletes(mergedAdds, deltaSeconds, deleteProbability)

	if e != nil {
		_ = fmt.Errorf("Error injecting deletes: %v\n", e)
		return
	}

	WriteToFile(outputFileName, injectedDeletes)
}

func InjectDeletes(inputLines []string, deltaSeconds int, deleteProbability float64) ([]string, error) {
	pq := &MinHeap{}
	heap.Init(pq)

	var outputLines []string

	for _, line := range inputLines {
		splitLine := strings.Fields(line)
		timestamp, err := strconv.Atoi(splitLine[2])
		if err != nil {
			fmt.Printf("Error parsing timestamp: %v\n", err)
			return nil, err
		}

		minEdge, ok := pq.Peek()
		for ; ok && (minEdge.timestamp+deltaSeconds < timestamp) && rand.Float64() <= deleteProbability; minEdge, ok = pq.Peek() {
			minEdge, _ = pq.Pop().(Edge)
			outputLines = append(outputLines, "D "+minEdge.sourceDest+" "+strconv.Itoa(minEdge.timestamp))
		}

		sourceDest := strings.Join(splitLine[:2], " ")

		heap.Push(pq, Edge{timestamp: timestamp, sourceDest: sourceDest})
		outputLines = append(outputLines, line)
	}

	return outputLines, nil
}

func MergeAdds(inputLines []string, deltaSeconds int, allowDuplicateEdge bool) ([]string, error) {
	mapAddTimestamp := make(map[string]int)

	var outputLines []string

	for _, line := range inputLines {
		splitLine := strings.Fields(line)
		timestamp, err := strconv.Atoi(splitLine[2])
		if err != nil {
			fmt.Printf("Error parsing timestamp: %v\n", err)
			return nil, err
		}
		sourceDest := strings.Join(splitLine[:2], " ")

		if ts, exist := mapAddTimestamp[sourceDest]; !exist || (allowDuplicateEdge && ts+deltaSeconds < timestamp) {
			mapAddTimestamp[sourceDest] = timestamp
			outputLines = append(outputLines, line)
		}
	}

	return outputLines, nil
}

func ReadFile(inputFile string) []string {
	inFile, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return nil
	}
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input file: %v\n", err)
	}

	return lines
}

func WriteToFile(outputFile string, lines []string) {
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	for _, line := range lines {
		_, err = writer.WriteString(line + "\n")
		if err != nil {
			fmt.Printf("Error writing to output file: %v\n", err)
			return
		}
	}
}
