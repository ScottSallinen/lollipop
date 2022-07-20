package graph

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
)

/// SendAdd: Direct add for debugging
func (g *Graph) SendAdd(srcRaw uint32, dstRaw uint32, weight float64) {
	g.ThreadStructureQ[g.RawIdToThreadIdx(srcRaw)] <- StructureChange{Type: ADD, SrcRaw: srcRaw, DstRaw: dstRaw, Weight: weight}
}

/// SendDel: Direct delete for debugging
func (g *Graph) SendDel(srcRaw uint32, dstRaw uint32) {
	g.ThreadStructureQ[g.RawIdToThreadIdx(srcRaw)] <- StructureChange{Type: DEL, SrcRaw: srcRaw, DstRaw: dstRaw}
}

func (g *Graph) DynamicEdgeDequeuer(queuechan chan RawEdge, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		//info("deq ", qElem.SrcRaw, qElem.DstRaw)
		g.ThreadStructureQ[g.RawIdToThreadIdx(qElem.SrcRaw)] <- StructureChange{Type: ADD, SrcRaw: qElem.SrcRaw, DstRaw: qElem.DstRaw, Weight: qElem.Weight}
	}
	deqWg.Done()
}

func (g *Graph) DynamicEdgeEnqueuer(graphName string, wg *sync.WaitGroup, idx uint64, enqCount uint64, result chan uint64) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint64(0)
	mLines := uint64(0)
	var lineText string
	var stringFields []string
	for scanner.Scan() {
		lines++
		if lines%enqCount != idx {
			continue
		}
		mLines++
		lineText = scanner.Text()
		if strings.HasPrefix(lineText, "#") {
			continue
		}
		stringFields = strings.Fields(lineText)
		sflen := len(stringFields)
		enforce.ENFORCE(sflen == 2 || sflen == 3)
		src, _ := strconv.Atoi(stringFields[0])
		dst, _ := strconv.Atoi(stringFields[1])

		weight := 1.0
		if sflen == 3 {
			weight, _ = strconv.ParseFloat(stringFields[2], 64)
		}
		stringFields = nil
		/*
			before, after, ok := strings.Cut(lineText, " ")
			if !ok {
				before, after, ok = strings.Cut(lineText, "\t")
				enforce.ENFORCE(ok)
			}
			src, _ := strconv.Atoi(before)
			dst, _ := strconv.Atoi(after)
			weight := 1.0
		*/

		//if src == dst {
		//	continue
		//}

		g.ThreadStructureQ[g.RawIdToThreadIdx(uint32(src))] <- StructureChange{Type: ADD, SrcRaw: uint32(src), DstRaw: uint32(dst), Weight: weight}
	}
	result <- mLines
	wg.Done()
}

func (g *Graph) LoadGraphDynamic(graphName string, feederWg *sync.WaitGroup) {
	//enqCount := mathutils.Max(uint32(THREADS/4), 1)
	enqCount := uint64(4)

	m1 := time.Now()

	resultchan := make(chan uint64, enqCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(enqCount))
	for i := uint64(0); i < enqCount; i++ {
		go g.DynamicEdgeEnqueuer(graphName, &enqWg, i, enqCount, resultchan)
	}
	enqWg.Wait()

	close(resultchan)
	lines := uint64(0)
	for e := range resultchan {
		lines += e
	}

	t1 := time.Since(m1)
	info("Read ", lines, " edges in (ms) ", t1.Milliseconds(), " fromWatch ", g.Watch.Elapsed())

	for i := 0; i < THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
}
