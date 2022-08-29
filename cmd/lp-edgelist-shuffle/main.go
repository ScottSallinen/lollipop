package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
	"github.com/ScottSallinen/lollipop/mathutils"
)

// TODO: handle edge properties

type EdgeProperty struct{}

func info(args ...interface{}) {
	log.Println("[Shuffler]\t", fmt.Sprint(args...))
}

func EdgeParser(lineText string) graph.RawEdge[EdgeProperty] {
	stringFields := strings.Fields(lineText)

	sflen := len(stringFields)
	enforce.ENFORCE(sflen == 2 || sflen == 3)

	src, _ := strconv.Atoi(stringFields[0])
	dst, _ := strconv.Atoi(stringFields[1])

	return graph.RawEdge[EdgeProperty]{SrcRaw: uint32(src), DstRaw: uint32(dst), EdgeProperty: EdgeProperty{}}
}

func EdgeDequeuer(queuechan chan graph.RawEdge[EdgeProperty], edgelist *[]graph.RawEdge[EdgeProperty], deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		*edgelist = append(*edgelist, graph.RawEdge[EdgeProperty]{SrcRaw: uint32(qElem.SrcRaw), DstRaw: uint32(qElem.DstRaw), EdgeProperty: EdgeProperty{}})
	}
	deqWg.Done()
}

func LoadEdgeList(graphName string, threads int) (finallist []graph.RawEdge[EdgeProperty]) {
	qCount := mathutils.MaxUint64(uint64(threads), 1)
	m1 := time.Now()

	edgelists := make([][]graph.RawEdge[EdgeProperty], threads)

	queuechans := make([]chan graph.RawEdge[EdgeProperty], qCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		queuechans[i] = make(chan graph.RawEdge[EdgeProperty], 4096)
		go EdgeDequeuer(queuechans[i], &edgelists[i], &deqWg)
	}

	resultchan := make(chan uint64, qCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		go graph.EdgeEnqueuer(queuechans, graphName, false, EdgeParser, &enqWg, i, qCount, qCount, resultchan)
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
