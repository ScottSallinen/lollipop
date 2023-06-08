package utils

import (
	"testing"
	"time"
)

func Test_Watch(t *testing.T) {
	watch := Watch{}

	watch.Start()
	time.Sleep(500 * time.Millisecond)
	dur := watch.Elapsed()
	if !FloatEquals(dur.Seconds(), 0.5, 0.05) {
		t.Error("seconds mismatch", dur.Seconds())
	}
	watch.Pause()
	time.Sleep(500 * time.Millisecond)
	dur2 := watch.Elapsed()
	if !FloatEquals(dur2.Seconds(), 0.5, 0.05) {
		t.Error("paused seconds mismatch", dur2.Seconds())
	}

	watch.UnPause()
	time.Sleep(500 * time.Millisecond)
	dur3 := watch.Elapsed()
	if !FloatEquals(dur3.Seconds(), 1, 0.05) {
		t.Error("unpaused seconds mismatch", dur3.Seconds())
	}

	dur4 := watch.AbsoluteElapsed()
	if !FloatEquals(dur4.Seconds(), 1.5, 0.05) {
		t.Error("absolute seconds mismatch", dur4.Seconds())
	}
}
