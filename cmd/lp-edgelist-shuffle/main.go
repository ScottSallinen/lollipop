package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...interface{}) {
	log.Println("[Shuffler]\t", fmt.Sprint(args...))
}

func EdgeDequeuer(queuechan chan graph.RawEdge, edgelist *[]graph.RawEdge, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		*edgelist = append(*edgelist, graph.RawEdge{SrcRaw: uint32(qElem.SrcRaw), DstRaw: uint32(qElem.DstRaw), Weight: qElem.Weight})
	}
	deqWg.Done()
}

func LoadEdgeList(graphName string, threads int) (finallist []graph.RawEdge) {
	qCount := mathutils.MaxUint64(uint64(threads), 1)
	m1 := time.Now()

	edgelists := make([][]graph.RawEdge, threads)

	queuechans := make([]chan graph.RawEdge, qCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		queuechans[i] = make(chan graph.RawEdge, 4096)
		go EdgeDequeuer(queuechans[i], &edgelists[i], &deqWg)
	}

	resultchan := make(chan uint64, qCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		go graph.EdgeEnqueuer(queuechans, graphName, false, &enqWg, i, qCount, qCount, resultchan)
	}
	enqWg.Wait()
	for i := uint64(0); i < qCount; i++ {
		close(queuechans[i])
	}
	close(resultchan)
	lines := uint64(0)
	for e := range resultchan {
		lines += e
	}
	deqWg.Wait()

	t1 := time.Since(m1)
	info("Read ", lines, " edges in (ms) ", t1.Milliseconds())

	for i := range edgelists {
		info(i, ":", len(edgelists[i]))
		finallist = append(finallist, edgelists[i]...)
	}
	return finallist
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()

	edgelist := LoadEdgeList(*gptr, *tptr)

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(edgelist), func(i, j int) { edgelist[i], edgelist[j] = edgelist[j], edgelist[i] })

	info("Edges:", len(edgelist))

	f, err := os.Create(*gptr + ".shuffled")
	enforce.ENFORCE(err)
	defer f.Close()
	for edge := range edgelist {
		// todo: weight?
		_, err := f.WriteString(fmt.Sprintf("%d %d\n", edgelist[edge].SrcRaw, edgelist[edge].DstRaw))
		enforce.ENFORCE(err)
	}
}
