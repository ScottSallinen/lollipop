package mathutils

import (
	"testing"
	"time"
)

func TestWatch(t *testing.T) {
	watch := Watch{}

	watch.Start()
	time.Sleep(2 * time.Second)
	dur := watch.Elapsed()
	if !FloatEquals(dur.Seconds(), 2, 0.1) {
		t.Error("seconds mismatch", dur.Seconds())
	}
	watch.Pause()
	time.Sleep(2 * time.Second)
	dur2 := watch.Elapsed()
	if !FloatEquals(dur2.Seconds(), 2, 0.1) {
		t.Error("paused seconds mismatch", dur2.Seconds())
	}

	watch.UnPause()
	time.Sleep(2 * time.Second)
	dur3 := watch.Elapsed()
	if !FloatEquals(dur3.Seconds(), 4, 0.1) {
		t.Error("unpaused seconds mismatch", dur3.Seconds())
	}

	dur4 := watch.AbsoluteElapsed()
	if !FloatEquals(dur4.Seconds(), 6, 0.1) {
		t.Error("absolute seconds mismatch", dur4.Seconds())
	}
}
