package graph

import (
	"bufio"
	"bytes"
	"os"
	"testing"

	"github.com/ScottSallinen/lollipop/utils"
)

// Benchmark a graph enqueue load

// 1236309 1306203  1 1175810400
var fileName = "/mnt/nvme0/data/wikipedia-growth.txt"
var target = 39000000

type MyEdge struct {
	WithTimestamp
	NoWeight
	NoRaw
}

func Benchmark_Load_RB_Bufio(b *testing.B) {
	edgeQueue := new(utils.RingBuffSPSC[TopologyEvent[MyEdge]])
	edgeQueue.Init(uint64(target))
	go discardInput(edgeQueue)

	file := utils.OpenFile(fileName)
	scanner := bufio.NewScanner(file)

	b.ResetTimer()

	stream_RB_bufio(scanner, edgeQueue, target)

	b.StopTimer()
	file.Close()
}

func Benchmark_Load_RB_Scan(b *testing.B) {
	edgeQueue := new(utils.RingBuffSPSC[TopologyEvent[MyEdge]])
	edgeQueue.Init(uint64(target))
	go discardInput(edgeQueue)

	file := utils.OpenFile(fileName)
	buf := [4096]byte{}
	scanner := utils.FastFileLines{
		Buf: buf[:],
	}

	b.ResetTimer()

	stream_RB_scan(file, &scanner, edgeQueue, target)

	b.StopTimer()
	file.Close()
}

/*
// Worse than chan?
func Benchmark_Load_Zenq_Scan(b *testing.B) {
	edgeQueue := zenq.New[TopologyEvent[MyEdge]](uint32(target))
	go discardInputZenq(edgeQueue)

	file := utils.OpenFile(fileName)
	scanner := utils.FastFileLines{}
	buf := [4096]byte{}
	scanner.Buf = buf[:]

	b.ResetTimer()

	stream_zenq_scan(file, &scanner, edgeQueue, target)

	b.StopTimer()
	file.Close()
}
*/

// Why so slow?
func Benchmark_Load_Chan_Scan(b *testing.B) {
	edgeQueue := make(chan TopologyEvent[MyEdge], (uint32(target)))
	go discardInputChan(edgeQueue)

	file := utils.OpenFile(fileName)
	scanner := utils.FastFileLines{}
	buf := [4096]byte{}
	scanner.Buf = buf[:]

	b.ResetTimer()

	stream_chan_scan(file, &scanner, edgeQueue, target)

	b.StopTimer()
	file.Close()
}

func stream_RB_bufio[EP EPP[E], E EPI[E]](scanner *bufio.Scanner, edgeQueue *utils.RingBuffSPSC[TopologyEvent[E]], n int) (lines uint64) {
	var sc TopologyEvent[E]
	var b []byte
	fields := make([]string, MAX_ELEMS_PER_EDGE)

	for ; lines < uint64(n); lines++ {
		if !scanner.Scan() {
			break
		}
		b = scanner.Bytes()

		utils.FastFields(fields, b)

		sc, _ = EdgeParser[E](fields)
		EP(&sc.EdgeProperty).ParseProperty(fields, -1, 1)

		if pos, ok := edgeQueue.PutFast(sc); !ok {
			edgeQueue.PutSlow(sc, pos)
		}
	}
	edgeQueue.Close()
	return lines
}

func stream_RB_scan[EP EPP[E], E EPI[E]](file *os.File, s *utils.FastFileLines, edgeQueue *utils.RingBuffSPSC[TopologyEvent[E]], n int) (lines uint64) {
	var sc TopologyEvent[E]
	var b []byte
	fields := make([]string, MAX_ELEMS_PER_EDGE)

	for ; lines < uint64(n); lines++ {
		if i := bytes.IndexByte(s.Buf[s.Start:s.End], '\n'); i >= 0 {
			b = s.Buf[s.Start : s.Start+i]
			s.Start += i + 1
		} else {
			if b = s.Scan(file); b == nil {
				break
			}
		}

		utils.FastFields(fields, b)

		sc, _ = EdgeParser[E](fields)
		EP(&sc.EdgeProperty).ParseProperty(fields, -1, 1)

		if pos, ok := edgeQueue.PutFast(sc); !ok {
			edgeQueue.PutSlow(sc, pos)
		}
	}
	edgeQueue.Close()
	return lines
}

func stream_chan_scan[EP EPP[E], E EPI[E]](file *os.File, s *utils.FastFileLines, edgeQueue chan TopologyEvent[E], n int) (lines uint64) {
	var sc TopologyEvent[E]
	var b []byte
	fields := make([]string, MAX_ELEMS_PER_EDGE)

	for ; lines < uint64(n); lines++ {
		if i := bytes.IndexByte(s.Buf[s.Start:s.End], '\n'); i >= 0 {
			b = s.Buf[s.Start : s.Start+i]
			s.Start += i + 1
		} else {
			if b = s.Scan(file); b == nil {
				break
			}
		}

		utils.FastFields(fields, b)

		sc, _ = EdgeParser[E](fields)
		EP(&sc.EdgeProperty).ParseProperty(fields, -1, 1)

		edgeQueue <- sc
	}
	close(edgeQueue)
	return lines
}

/*
func stream_zenq_scan[EP EPP[E], E EPI[E]](file *os.File, s *utils.FastFileLines, edgeQueue *zenq.ZenQ[TopologyEvent[E]], n int) (lines uint64) {
	var sc TopologyEvent[E]
	var b []byte
	fields := make([]string, MAX_ELEMS_PER_EDGE)

	for ; lines < uint64(n); lines++ {
		if i := bytes.IndexByte(s.Buf[s.Start:s.End], '\n'); i >= 0 {
			b = s.Buf[s.Start : s.Start+i]
			s.Start += i + 1
		} else {
			if b = s.Scan(file); b == nil {
				break
			}
		}

		utils.FastFields(fields, b)

		sc, _ = EdgeParser[E](fields)
		EP(&sc.EdgeProperty).ParseProperty(fields, -1, 1)

		edgeQueue.Write(sc)
	}
	edgeQueue.Close()
	return lines
}

func discardInputZenq(edgeQueue *zenq.ZenQ[TopologyEvent[MyEdge]]) {
	for {
		edgeQueue.Read()
	}
}
*/

func discardInputChan(edgeQueue chan TopologyEvent[MyEdge]) {
	for range edgeQueue {
		// do nothing
	}
}

func discardInput(edgeQueue *utils.RingBuffSPSC[TopologyEvent[MyEdge]]) {
	retried := 0
	totalRetried := 0
	var ok bool
	var closed bool
	var pos uint64

outer:
	for {
		if _, ok, pos = edgeQueue.GetFast(); !ok {
			if _, closed, retried = edgeQueue.GetSlow(pos); closed {
				break outer
			}
			totalRetried += retried
		}
	}
}
