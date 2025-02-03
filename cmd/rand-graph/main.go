package main

func main() {
	n := 5  // Number of nodes
	m := 25 // Number of edges

	g := GenerateRandomGraph(n, m, "./cmd/rand-graph/rand-graph.txt", "./cmd/rand-graph/rand-graph-sssp.json")
	PrintGraph(g)
}
