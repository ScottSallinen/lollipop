package utils

import (
	"sync"
	"time"
)

type Watch struct {
	mu           sync.RWMutex
	paused       bool
	pauseTime    time.Time
	startTime    time.Time
	adjustedTime time.Time
}

func (w *Watch) Start() {
	w.mu.Lock()
	w.startTime = time.Now()
	w.adjustedTime = w.startTime
	if w.paused {
		panic("watch cant start because paused")
	}
	w.mu.Unlock()
}

func (w *Watch) Elapsed() time.Duration {
	w.mu.RLock()
	mNow := time.Now()
	if w.paused {
		diff := (mNow.Sub(w.adjustedTime) - mNow.Sub(w.pauseTime))
		w.mu.RUnlock()
		return diff
	}
	mAdjStart := w.adjustedTime
	w.mu.RUnlock()
	return mNow.Sub(mAdjStart)
}

func (w *Watch) AbsoluteElapsed() time.Duration {
	w.mu.RLock()
	mNow := time.Now()
	mStart := w.startTime
	w.mu.RUnlock()
	return mNow.Sub(mStart)
}

func (w *Watch) Pause() time.Duration { // returns currently elapsed time
	w.mu.Lock()
	if !w.paused {
		w.pauseTime = time.Now()
		w.paused = true
	} else {
		panic("watch already paused")
	}
	w.mu.Unlock()
	return w.pauseTime.Sub(w.adjustedTime)
}

func (w *Watch) UnPause() {
	w.mu.Lock()
	if w.paused {
		w.paused = false
		w.adjustedTime = w.adjustedTime.Add(time.Since(w.pauseTime))
	} else {
		panic("watch wasn't paused")
	}
	w.mu.Unlock()
}
