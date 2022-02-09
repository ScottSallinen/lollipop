package framework

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
	"github.com/ScottSallinen/lollipop/graph"
)

func OnQueueVisitAsyncPMP(g *graph.Graph, sidx uint32, didx uint32, VisitData interface{}) {
	g.MessageQ[g.Vertices[didx].ToThreadIdx()] <- graph.Message{Sidx: sidx, Didx: didx, Val: VisitData.(float64)}
	g.MsgSend[g.Vertices[sidx].ToThreadIdx()] += 1
}

func (frame *Framework) AsyncPMPConverge(g *graph.Graph) {
	var wg sync.WaitGroup
	VOTES := graph.THREADS + 1
	exit := false
	wg.Add(VOTES)
	termVotes := make([]int, VOTES)
	termData := make([]uint32, VOTES)

	go func(exit *bool) {
		for !*exit && false {
			result := uint32(0)
			for _, v := range termData {
				result += v
			}
			info(termData, result)
			time.Sleep(1 * time.Second)
		}
	}(&exit)

	go func() {
		termData[VOTES-1] = uint32(len(g.Vertices))
		for vidx := range g.Vertices {
			g.MessageQ[vidx%graph.THREADS] <- graph.Message{Sidx: 0, Didx: uint32(vidx), Val: 1.0}
		}
		termVotes[VOTES-1] = 1
		wg.Done()
	}()

	for i := 0; i < graph.THREADS; i++ {
		go func(idx int, wg *sync.WaitGroup) {
			msgs := make(map[uint32]float64, 0)
			termVotes[idx] = -1
			for {
				select {
				case msg, ok := <-g.MessageQ[idx]:
					if ok {
						msgs[msg.Didx] += msg.Val
						//frame.OnVisitVertex(g, msg.Dst, msg.Val)
						g.MsgRecv[idx] -= 1
					} else {
						info("Channel closed!")
					}
				default:
					if len(msgs) != 0 {
						termVotes[idx] = -1
						for k, v := range msgs {
							frame.OnVisitVertex(g, k, v)
						}
						msgs = make(map[uint32]float64, 0)
						continue
					}
					termData[idx] = g.MsgSend[idx] + g.MsgRecv[idx]
					termVotes[idx] = 1
					allDone := 0
					allMsgs := uint32(0)
					for v := 0; v < VOTES; v++ {
						allDone += termVotes[v]
						allMsgs += termData[v]
					}
					if allDone == VOTES && allMsgs == 0 {
						wg.Done()
						return
					}
				}
			}
		}(i, &wg)
	}
	wg.Wait()
	exit = true

	inFlight := uint32(0)
	for v := 0; v < VOTES; v++ {
		//info(v, ":", termData[v])
		inFlight += termData[v]
	}
	enforce.ENFORCE(inFlight == 0)

	for i := 0; i < graph.THREADS; i++ {
		select {
		case t, ok := <-g.MessageQ[i]:
			if ok {
				info(i, " ", t, "LEFTOVERS?")
			} else {
				info(i, "channel closed!")
			}
		default:
		}
	}
}
