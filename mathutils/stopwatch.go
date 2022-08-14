package mathutils

import (
	"sync"
	"time"

	"github.com/ScottSallinen/lollipop/enforce"
)

type Watch struct {
	Mu           sync.RWMutex
	Paused       bool
	PauseTime    time.Time
	StartTime    time.Time
	AdjustedTime time.Time
}

func (w *Watch) Start() {
	w.Mu.Lock()
	w.StartTime = time.Now()
	w.AdjustedTime = w.StartTime
	if w.Paused {
		enforce.FAIL("watch cant start because paused")
	}
	w.Mu.Unlock()
}

func (w *Watch) Elapsed() time.Duration {
	w.Mu.RLock()
	mNow := time.Now()
	if w.Paused {
		diff := (mNow.Sub(w.AdjustedTime) - mNow.Sub(w.PauseTime))
		w.Mu.RUnlock()
		return diff
	}
	mAdjStart := w.AdjustedTime
	w.Mu.RUnlock()
	return mNow.Sub(mAdjStart)
}

func (w *Watch) AbsoluteElapsed() time.Duration {
	w.Mu.RLock()
	mNow := time.Now()
	mStart := w.StartTime
	w.Mu.RUnlock()
	return mNow.Sub(mStart)
}

func (w *Watch) Pause() {
	w.Mu.Lock()
	if !w.Paused {
		w.PauseTime = time.Now()
		w.Paused = true
	} else {
		enforce.FAIL("watch already paused")
	}
	w.Mu.Unlock()
}

func (w *Watch) UnPause() {
	w.Mu.Lock()
	if w.Paused {
		w.Paused = false
		w.AdjustedTime = w.AdjustedTime.Add(time.Since(w.PauseTime))
	} else {
		enforce.FAIL("watch wasnt paused")
	}
	w.Mu.Unlock()
}
