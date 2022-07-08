package mathutils

import (
	"math"
	"sort"
	"sync/atomic"
	"unsafe"

	"golang.org/x/exp/constraints"
)

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

func Median(n []int) int {
	sort.Ints(n) // sort numbers
	idx := len(n) / 2
	if len(n)%2 == 0 { // even
		return n[idx]
	}
	return (n[idx-1] + n[idx]) / 2
}

func AtomicAddFloat64(val *float64, delta float64) (new float64, old float64) {
	for {
		old = *val
		new = old + delta
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
