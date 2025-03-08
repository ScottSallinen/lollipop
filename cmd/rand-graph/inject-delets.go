package main

import (
	"bufio"
	"container/heap"
	"fmt"
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

	inputFile := os.Args[1]
	outputFile := os.Args[2]
	deltaDaysString := os.Args[3]
	deltaDays, _ := strconv.Atoi(deltaDaysString)
	deltaSeconds := deltaDays * 24 * 60 * 60
	injectDeletes(inputFile, outputFile, deltaSeconds)
	//MergeAdds(inputFile, outputFile, deltaSeconds)
}

func injectDeletes(inputFile, outputFile string, deltaSeconds int) {
	inFile, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inFile.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	pq := &MinHeap{}
	heap.Init(pq)

	for scanner.Scan() {
		line := scanner.Text()
		splitLine := strings.Fields(line)
		timestamp, err := strconv.Atoi(splitLine[2])
		if err != nil {
			fmt.Printf("Error parsing timestamp: %v\n", err)
			return
		}

		minEdge, ok := pq.Peek()
		for ; ok && minEdge.timestamp+deltaSeconds < timestamp; minEdge, ok = pq.Peek() {
			minEdge, _ = pq.Pop().(Edge)
			_, err := writer.WriteString("D " + minEdge.sourceDest + " " + strconv.Itoa(minEdge.timestamp+deltaSeconds) + "\n")
			if err != nil {
				fmt.Printf("Error writing to output file: %v\n", err)
				return
			}
		}

		sourceDest := strings.Join(splitLine[:2], " ")

		heap.Push(pq, Edge{timestamp: timestamp, sourceDest: sourceDest})

		_, err = writer.WriteString(line + "\n")
		if err != nil {
			fmt.Printf("Error writing to output file: %v\n", err)
			return
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input file: %v\n", err)
	}

}

func MergeAdds(inputFile, outputFile string, deltaSeconds int) {
	inFile, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inFile.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	mapAddTimestamp := make(map[string]int)

	for scanner.Scan() {
		line := scanner.Text()
		splitLine := strings.Fields(line)
		timestamp, err := strconv.Atoi(splitLine[2])
		if err != nil {
			fmt.Printf("Error parsing timestamp: %v\n", err)
			return
		}
		sourceDest := strings.Join(splitLine[:2], " ")

		if ts, exist := mapAddTimestamp[sourceDest]; !exist || ts+deltaSeconds < timestamp {
			mapAddTimestamp[sourceDest] = timestamp
			_, err = writer.WriteString(line + "\n")
			if err != nil {
				fmt.Printf("Error writing to output file: %v\n", err)
				return
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input file: %v\n", err)
	}

}
