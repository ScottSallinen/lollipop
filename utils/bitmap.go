package utils

import (
	"math/bits"
)

// Initially inspired from https://github.com/kelindar/bitmap Thank you for using the MIT license!
// Mostly just implementing/changing for the needed use-cases.

// ------------------ Regular Bitmap ------------------

type Bitmap []uint64

// Inline-able, returns false if out of range.
func (bitmap *Bitmap) QuickSet(x uint32) bool {
	idx := int(x >> 6)
	bit := int(x % 64)
	if idx >= len(*bitmap) {
		return false
	}
	(*bitmap)[idx] |= (1 << bit)
	return true
}

// Set sets the bit x in the bitmap and grows it if necessary.
func (bitmap *Bitmap) Set(x uint32) {
	idx := int(x >> 6)
	bit := int(x % 64)
	if idx >= len(*bitmap) {
		bitmap.grow(idx)
	}
	(*bitmap)[idx] |= (1 << bit)
}

// Zeros all bits in the bitmap.
func (bitmap *Bitmap) Zeroes() {
	for i := 0; i < len(*bitmap); i++ {
		(*bitmap)[i] = 0
	}
}

// Grow grows the bitmap size until we reach the desired bit.
func (bitmap *Bitmap) Grow(desiredBit uint32) {
	// The capacity check should be first, because this way it gets inlined.
	idx := int(desiredBit >> 6)
	if idx > len(*bitmap) || *bitmap == nil {
		bitmap.grow(idx)
	}
}

// Grow grows the size of the bitmap until we reach the desired block offset
func (bitmap *Bitmap) grow(idx int) {
	// If there's space, resize the slice without copying.
	if cap(*bitmap) > idx {
		*bitmap = (*bitmap)[:idx+1]
		return
	}
	old := *bitmap
	*bitmap = make(Bitmap, idx+1, resize(cap(old), idx+1))
	copy(*bitmap, old)
}

// Finds the smallest unused index. Might be the size of the bitmap (i.e., all bits are set).
func (bitmap Bitmap) FirstUnused() (pos uint32) {
	for i := 0; i < len(bitmap); i++ {
		if bitmap[i] != 0xFFFFFFFFFFFFFFFF {
			return uint32(i<<6 + bits.TrailingZeros64(^bitmap[i]))
		}
	}
	return uint32(len(bitmap) << 6)
}

// resize calculates the new required capacity and a new index
func resize(capacity, v int) int {
	const threshold = 256

	if v < threshold {
		return int(RoundUpPow(uint64(v + 1)))
	}

	if capacity < threshold {
		capacity = threshold
	}

	for 0 < capacity && capacity < (v+1) {
		capacity += (capacity + 3*threshold) / 4
	}
	return capacity
}
