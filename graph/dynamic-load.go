package graph

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/mathutils"
)

// Direct add for debugging
func (g *Graph) SendAdd(srcRaw uint32, dstRaw uint32) {
	g.ThreadStructureQ[g.RawIdToThreadIdx(srcRaw)] <- StructureChange{Type: ADD, SrcRaw: srcRaw, DstRaw: dstRaw}
}

// Direct delete for debugging
func (g *Graph) SendDel(srcRaw uint32, dstRaw uint32) {
	g.ThreadStructureQ[g.RawIdToThreadIdx(srcRaw)] <- StructureChange{Type: DEL, SrcRaw: srcRaw, DstRaw: dstRaw}
}

func (g *Graph) DynamicEdgeDequeuer(queuechan chan QueueElem, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		//info("deq ", qElem.SrcRaw, qElem.DstRaw)
		g.ThreadStructureQ[g.RawIdToThreadIdx(qElem.SrcRaw)] <- StructureChange{Type: ADD, SrcRaw: qElem.SrcRaw, DstRaw: qElem.DstRaw}
	}
	deqWg.Done()
	return
}

func (g *Graph) LoadGraphDynamic(graphName string, feederWg *sync.WaitGroup) {
	deqCount := mathutils.MaxUint32(uint32(THREADS), 1)
	enqCount := mathutils.MaxUint32(uint32(THREADS/2), 1)

	m1 := time.Now()

	var deqWg sync.WaitGroup

	deqWg.Add(int(deqCount))
	queuechans := make([]chan QueueElem, deqCount)
	for i := uint32(0); i < deqCount; i++ {
		queuechans[i] = make(chan QueueElem, 256)
		go g.DynamicEdgeDequeuer(queuechans[i], &deqWg)
	}

	resultchan := make(chan uint32, enqCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(enqCount))
	for i := uint32(0); i < enqCount; i++ {
		go g.EdgeEnqueuer(queuechans, graphName, &enqWg, i, enqCount, deqCount, resultchan)
	}
	enqWg.Wait()
	for i := uint32(0); i < deqCount; i++ {
		close(queuechans[i])
	}
	close(resultchan)
	lines := uint32(0)
	for e := range resultchan {
		lines += e
	}

	t1 := time.Since(m1)
	info("Read ", lines, " edges in ", t1)

	deqWg.Wait()
	info("Dequeue complete, no more work to do.")
	for i := 0; i < THREADS; i++ {
		close(g.ThreadStructureQ[i])
	}
	feederWg.Done()
}
