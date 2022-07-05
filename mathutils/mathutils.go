package mathutils

import (
	"math"
	"sort"
	"sync/atomic"
	"unsafe"
)

func FloatEquals(a float64, b float64, args ...interface{}) bool {
	if len(args) >= 1 {
		return math.Abs(a-b) < args[0].(float64)
	}
	return math.Abs(a-b) < 0.001
}

func MaxUint64(x, y uint64) uint64 {
	if x < y {
		return y
	}
	return x
}
func MaxUint32(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}
func MaxFloat64(x, y float64) float64 {
	if x < y {
		return y
	}
	return x
}
func MinFloat64(x, y float64) float64 {
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
func MedianFloat64(n []float64) float64 {
	sort.Float64s(n) // sort numbers
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
