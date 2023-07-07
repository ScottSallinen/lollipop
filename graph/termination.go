package graph

import (
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ScottSallinen/lollipop/utils"
)

// DEBUG func to periodically print termination data and vote status.
func (g *Graph[V, E, M, N]) PrintTerminationStatus(exit *bool) {
	time.Sleep(time.Duration(g.Options.PollingRate) * time.Millisecond)
	for !*exit {
		g.printStatus("Active: ")
		time.Sleep(time.Duration(g.Options.PollingRate) * time.Millisecond)
	}
	g.printStatus("Finals: ")
}

func (g *Graph[V, E, M, N]) printStatus(prefix string) {
	chkRes := int64(0)
	for t := 0; t < int(g.NumThreads); t++ {
		chkRes += int64(g.GraphThreads[t].MsgSend) - int64(g.GraphThreads[t].MsgRecv)
	}
	log.Info().Msg(prefix + " Outstanding: " + utils.V(chkRes))
}

// Checks for messages being consistent across threads, and if so, tries to terminate.
// 99% of the time it works every time!
func (g *Graph[V, E, M, N]) CheckTermination(tidx uint16) bool {
	THREADS := g.NumThreads
	// Report all actions we have taken.
	g.TerminateData[tidx] = (int64(g.GraphThreads[tidx].MsgSend) + int64(g.GraphThreads[tidx].MsgRecv))

	// Compute our view of all actions, based on whatever was previously reported by others.
	allActions := int64(0)
	for t := 0; t < int(THREADS); t++ {
		allActions += g.TerminateData[t]
	}

	// Report our view.
	if g.TerminateView[tidx] != allActions {
		g.TerminateView[tidx] = allActions
		g.TerminateVotes[tidx] = 0 // View mismatch; do not attempt. We changed our own view.
		return false
	}

	// Check all reported terminate views to see if they match our view of all actions.
	for t := 0; t < int(THREADS); t++ {
		if g.TerminateView[t] != allActions {
			// View mismatch; do not attempt. Someone else does not agree with our view.
			g.TerminateVotes[tidx] = 0
			return false
		}
	}

	// We believe everyone else to have the same view of all action count; begin voting procedure.
	if g.TerminateVotes[tidx] == 0 { // Do not overwrite state > 0 if not needed.
		g.TerminateVotes[tidx] = 1 // State 1: We wish to begin termination.
	}

	// Check our view of other thread states.
	for t := 0; t < int(THREADS); t++ {
		if g.TerminateVotes[t] == 0 {
			// In case we were ready, we no longer will be, as we no longer view everyone in at least state 1.
			g.TerminateVotes[tidx] = 1
			return false
		}
	}

	// We think everyone is voting at least >= 1. Flag to 2 to suggest we view everyone else is voting at least 1.
	if g.TerminateVotes[tidx] == 1 {
		g.TerminateVotes[tidx] = 2
		return false // We are now in state 2, but we need to go check for messages.
	}

	for t := 0; t < int(THREADS); t++ {
		if g.TerminateVotes[t] < 2 { // Ensure everyone is ready in state 2.
			if g.TerminateVotes[tidx] == 3 { // Fall back if we were in state 3 and view someone no longer ready. // TODO: Is this needed?
				if g.TerminateVotes[t] == 0 {
					g.TerminateVotes[tidx] = 1
				} else { // g.TerminateVotes[t] == 1
					g.TerminateVotes[tidx] = 2
				}
			}
			return false // We no longer think everyone is in state 2.
		}
	}

	// We now move to state 3: we previously saw everyone is 2, and we have re-checked our own messages and still believe all are 2.
	// If we had a new message when we re-checked (or someone else did), we should have already fallen back to 0 and not come back to this spot...
	g.TerminateVotes[tidx] = 3
	for t := 0; t < int(THREADS); t++ {
		if g.TerminateVotes[t] != 3 { // Ensure everyone is in state 3.
			if g.TerminateVotes[t] < 2 { // TODO: is this possible?
				log.Warn().Msg("WARNING T[" + utils.F("%02d", tidx) + "] FAILURE at stage 3, observing a stage: " + utils.V(g.TerminateVotes[t]) + ". thread: " + utils.V(t))
			}
			return false
		}
	}
	return true
}

// Ensure queues are empty and no messages are inflight.
func (g *Graph[V, E, M, N]) EnsureCompleteness() {
	msgSend := uint64(0)
	msgRecv := uint64(0)
	for t := 0; t < int(g.NumThreads); t++ {
		msgSend += g.GraphThreads[t].MsgSend
		msgRecv += g.GraphThreads[t].MsgRecv
	}
	inFlight := int64(msgSend) - int64(msgRecv)
	if inFlight != 0 {
		log.Panic().Msg("Messages in flight: " + utils.V(inFlight))
	}

	for t := 0; t < int(g.NumThreads); t++ {
		_, ok := g.GraphThreads[t].NotificationQueue.Accept()
		if ok {
			log.Panic().Msg("Incomplete: leftover messages?")
		}
	}
}
