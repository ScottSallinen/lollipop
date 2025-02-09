package main

func main() {
	n := 50   // Number of nodes
	m := 1000 // Number of edges

	_ = GenerateRandomGraph(n, m, "./cmd/rand-graph/rand-graph.txt", "./cmd/rand-graph/rand-graph-sssp.json")
	//PrintGraph(g)
}
