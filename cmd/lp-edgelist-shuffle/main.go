package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/mathutils"
)

func info(args ...interface{}) {
	log.Println("[Shuffler]\t", fmt.Sprint(args...))
}

func LineDequeuer(queuechan chan string, lineList *[]string, deqWg *sync.WaitGroup) {
	for qElem := range queuechan {
		*lineList = append(*lineList, qElem)
	}
	deqWg.Done()
}

func LineEnqueuer(queuechans []chan string, graphName string, undirected bool, wg *sync.WaitGroup, idx uint64, enqCount uint64, deqCount uint64, result chan uint64) {
	file, err := os.Open(graphName)
	enforce.ENFORCE(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := uint64(0)
	mLines := uint64(0)
	for scanner.Scan() {
		lines++
		if lines%enqCount != idx {
			continue
		}
		mLines++
		lineText := scanner.Text()
		if strings.HasPrefix(lineText, "#") {
			continue
		}
		queuechans[lines%deqCount] <- lineText
	}
	result <- mLines
	wg.Done()
}

func LoadlineList(graphName string, threads int) (finallist []string) {
	qCount := mathutils.MaxUint64(uint64(threads), 1)
	m1 := time.Now()

	lineLists := make([][]string, threads)

	queuechans := make([]chan string, qCount)
	var deqWg sync.WaitGroup
	deqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		queuechans[i] = make(chan string, 4096)
		go LineDequeuer(queuechans[i], &lineLists[i], &deqWg)
	}

	resultchan := make(chan uint64, qCount)
	var enqWg sync.WaitGroup
	enqWg.Add(int(qCount))
	for i := uint64(0); i < qCount; i++ {
		go LineEnqueuer(queuechans, graphName, false, &enqWg, i, qCount, qCount, resultchan)
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
	info("Read ", lines, " lines in (ms) ", t1.Milliseconds())

	for i := range lineLists {
		info(i, ":", len(lineLists[i]))
		finallist = append(finallist, lineLists[i]...)
	}
	return finallist
}

func main() {
	gptr := flag.String("g", "data/test.txt", "Graph file")
	tptr := flag.Int("t", 32, "Thread count")
	flag.Parse()

	lineList := LoadlineList(*gptr, *tptr)

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(lineList), func(i, j int) { lineList[i], lineList[j] = lineList[j], lineList[i] })

	info("Lines:", len(lineList))

	f, err := os.Create(*gptr + ".shuffled")
	enforce.ENFORCE(err)
	defer f.Close()
	for line := range lineList {
		_, err := f.WriteString(fmt.Sprintf("%s\n", lineList[line]))
		enforce.ENFORCE(err)
	}
}
