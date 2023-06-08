package utils

import (
	"strconv"
	"testing"
)

// testData contains the number of items to add to the queues in each test.
type testData struct {
	count int
}

var (
	tests = []testData{
		{count: 0},
		{count: 1},
		{count: 10},
		{count: 100},
		{count: 1000},    // 1k
		{count: 10000},   //10k
		{count: 100000},  // 100k
		{count: 1000000}, // 1mi
	}
)

// A given benchmark included in https://github.com/ef-ds/deque
func BenchmarkMicroserviceQueue(b *testing.B) {
	for i, test := range tests {
		// Doesn't run the first (0 items) as 0 items makes no sense for this test.
		if i == 0 {
			continue
		}

		b.Run(strconv.Itoa(test.count), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				d := Deque[*testData]{}

				// Simulate stable traffic
				for i := 0; i < test.count; i++ {
					d.PushBack(nil)
					d.PopFront()
				}

				// Simulate slowly increasing traffic
				for i := 0; i < test.count; i++ {
					d.PushBack(nil)
					d.PushBack(nil)
					d.PopFront()
				}

				// Simulate slowly decreasing traffic, bringing traffic back to normal
				for i := 0; i < test.count; i++ {
					d.PopFront()
					if d.Len() > 0 {
						d.PopFront()
					}
					d.PushBack(nil)
				}

				// Simulate quick traffic spike (DDOS attack, etc)
				for i := 0; i < test.count; i++ {
					d.PushBack(nil)
				}

				// Simulate stable traffic while at high traffic
				for i := 0; i < test.count; i++ {
					d.PushBack(nil)
					d.PopFront()
				}

				// Simulate going back to normal (DDOS attack fended off)
				for i := 0; i < test.count; i++ {
					d.PopFront()
				}

				// Simulate stable traffic (now that is back to normal)
				for i := 0; i < test.count; i++ {
					d.PushBack(nil)
					d.PopFront()
				}
			}
		})
	}
}

// Checking performance improvement of using inlined variants...
func BenchmarkMicroserviceQueueInline(b *testing.B) {
	for i, test := range tests {
		// Doesn't run the first (0 items) as 0 items makes no sense for this test.
		if i == 0 {
			continue
		}

		b.Run(strconv.Itoa(test.count), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				d := Deque[*testData]{}

				// Simulate stable traffic
				for i := 0; i < test.count; i++ {
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
					if _, ok := d.TryPopFront(); ok {
						d.UpdatePopFront()
					}
				}

				// Simulate slowly increasing traffic
				for i := 0; i < test.count; i++ {
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
					if _, ok := d.TryPopFront(); ok {
						d.UpdatePopFront()
					}
				}

				// Simulate slowly decreasing traffic, bringing traffic back to normal
				for i := 0; i < test.count; i++ {
					if _, ok := d.TryPopFront(); ok {
						d.UpdatePopFront()
					}
					if d.Len() > 0 {
						if _, ok := d.TryPopFront(); ok {
							d.UpdatePopFront()
						}
					}
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
				}

				// Simulate quick traffic spike (DDOS attack, etc)
				for i := 0; i < test.count; i++ {
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
				}

				// Simulate stable traffic while at high traffic
				for i := 0; i < test.count; i++ {
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
					if _, ok := d.TryPopFront(); ok {
						d.UpdatePopFront()
					}
				}

				// Simulate going back to normal (DDOS attack fended off)
				for i := 0; i < test.count; i++ {
					if _, ok := d.TryPopFront(); ok {
						d.UpdatePopFront()
					}
				}

				// Simulate stable traffic (now that is back to normal)
				for i := 0; i < test.count; i++ {
					if !d.FastPushBack(nil) {
						d.SlowPushBack(nil)
					}
					if _, ok := d.TryPopFront(); ok {
						d.UpdatePopFront()
					}
				}
			}
		})
	}
}
