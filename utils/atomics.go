package utils

import (
	"sync/atomic"
	"unsafe"
)

//go:nosplit
func Noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

//go:nosplit
func AtomicAddFloat64(targetVal *float64, delta float64) (oldF float64, oldU uint64, newU uint64) {
	for {
		oldU = float64Bits(*targetVal)
		oldF = float64FromBits(oldU)
		newU = float64Bits(oldF + delta)
		if atomic.CompareAndSwapUint64((*uint64)(Noescape(unsafe.Pointer(targetVal))), oldU, newU) {
			return
		}
	}
}

//go:nosplit
func AtomicAddFloat64U(targetVal *float64, delta float64) (oldU uint64, newU uint64) {
	for {
		oldU = float64Bits(*targetVal)
		newU = float64Bits(float64FromBits(oldU) + delta)
		if atomic.CompareAndSwapUint64((*uint64)(Noescape(unsafe.Pointer(targetVal))), oldU, newU) {
			return
		}
	}
}

//go:nosplit
func AtomicSwapFloat64(targetVal *float64, new float64) float64 {
	newU := float64Bits(new)
	for {
		oldU := float64Bits(*targetVal)
		if atomic.CompareAndSwapUint64((*uint64)(Noescape(unsafe.Pointer(targetVal))), oldU, newU) {
			return *(*float64)(Noescape(unsafe.Pointer(&oldU)))
		}
	}
}

//go:nosplit
func AtomicMinUint32(targetVal *uint32, new uint32) (old uint32) {
	for {
		old = atomic.LoadUint32(targetVal)
		if new >= old || atomic.CompareAndSwapUint32(targetVal, old, new) {
			return old
		}
	}
}

//go:nosplit
func AtomicMaxUint64(targetVal *uint64, new uint64) (old uint64) {
	for {
		old = atomic.LoadUint64(targetVal)
		if new <= old || atomic.CompareAndSwapUint64(targetVal, old, new) {
			return old
		}
	}
}

//go:nosplit
func AtomicMinFloat64[T ~float64](targetVal *T, new T) (oldF T) {
	for {
		oldU := *(*uint64)(Noescape(unsafe.Pointer(targetVal)))
		oldF = floatFromBits[T](oldU)
		if new >= oldF {
			return oldF
		} else if atomic.CompareAndSwapUint64((*uint64)(Noescape(unsafe.Pointer(targetVal))), oldU, floatBits(new)) {
			return oldF
		}
	}
}

//go:nosplit
func AtomicLoadFloat64[T ~float64](targetVal *T) T {
	return floatFromBits[T](atomic.LoadUint64((*uint64)(Noescape(unsafe.Pointer(targetVal)))))
}

//go:nosplit
func floatFromBits[T ~float64](b uint64) T {
	return *(*T)((unsafe.Pointer(&b)))
}

//go:nosplit
func floatBits[T ~float64](f T) uint64 {
	return *(*uint64)((unsafe.Pointer(&f)))
}

//go:nosplit
func float64Bits(f float64) uint64 {
	return *(*uint64)((unsafe.Pointer(&f)))
}

//go:nosplit
func float64FromBits(b uint64) float64 {
	return *(*float64)((unsafe.Pointer(&b)))
}
