package mathutils

import (
	"github.com/ScottSallinen/lollipop/enforce"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/exp/constraints"
)

type Pair[F any, S any] struct {
	First  F
	Second S
}

func FloatEquals(a float64, b float64, args ...interface{}) bool {
	if len(args) >= 1 {
		return math.Abs(a-b) < args[0].(float64)
	}
	return math.Abs(a-b) < 0.001
}

func Max[T constraints.Ordered](x, y T) T {
	if x < y {
		return y
	}
	return x
}

func Min[T constraints.Ordered](x, y T) T {
	if y < x {
		return y
	}
	return x
}

func Sum[T constraints.Integer | constraints.Float](slice []T) T {
	sum := T(0)
	for _, n := range slice {
		sum += n
	}
	return sum
}

func Median(n []int) int {
	sort.Ints(n) // sort numbers
	idx := len(n) / 2
	if len(n)%2 == 0 { // even
		return n[idx]
	}
	return (n[idx-1] + n[idx]) / 2
}

func MinMax[T constraints.Ordered](slice []T) (T, T) {
	max := slice[0]
	min := slice[0]
	for _, e := range slice {
		if max < e {
			max = e
		}
		if min > e {
			min = e
		}
	}
	return min, max
}

func SampleVariance[T constraints.Integer | constraints.Float](slice []T, mean float64) float64 {
	enforce.ENFORCE(len(slice) > 0)
	variance := float64(0)
	for _, l := range slice {
		variance += (float64(l) - mean) * (float64(l) - mean)
	}
	return variance / float64(len(slice)-1)
}

func AtomicAddFloat64(val *float64, delta float64) (old float64) {
	for {
		old = *val
		new := old + delta
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(val)),
			math.Float64bits(old),
			math.Float64bits(new),
		) {
			break
		}
	}
	return
}

func AtomicSwapFloat64(val *float64, new float64) (old float64) {
	for {
		old = *val
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(val)),
			math.Float64bits(old),
			math.Float64bits(new),
		) {
			break
		}
	}
	return
}

type IndexedFloat64Slice struct {
	sort.Float64Slice
	Idx []int
}

func (s IndexedFloat64Slice) Swap(i, j int) {
	s.Float64Slice.Swap(i, j)
	s.Idx[i], s.Idx[j] = s.Idx[j], s.Idx[i]
}

func NewIndexedFloat64Slice(n []float64) *IndexedFloat64Slice {
	cpy := make([]float64, len(n))
	copy(cpy, n)
	s := &IndexedFloat64Slice{Float64Slice: sort.Float64Slice(cpy), Idx: make([]int, len(n))}
	for i := range s.Idx {
		s.Idx[i] = i
	}
	return s
}

// BatchParallelFor will create threads to chunk the size into batches, and
// execute the function given on each ordinal integer up to size.
// The applicator func is provided the index in size, and the thread id
// that is executing the applicator func (useful for accumulator purposes)
func BatchParallelFor(size int, threads int, applicator func(int, int)) {
	var wg sync.WaitGroup
	wg.Add(threads)
	batch := size / threads
	for t := 0; t < threads; t++ {
		go func(tidx int) {
			defer wg.Done()
			start := tidx * batch
			end := (tidx + 1) * batch
			if tidx == (threads - 1) {
				end = size
			}
			for j := start; j < end; j++ {
				applicator(j, tidx)
			}
		}(t)
	}
	wg.Wait()
}

func RemoveRandomElement[T any](slice []T) (T, []T) {
	idx := rand.Intn(len(slice))
	ret := slice[idx]
	slice[idx] = slice[len(slice)-1]
	return ret, slice[:len(slice)-1]
}

// Copied from https://stackoverflow.com/a/32843750
func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) (complete bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
