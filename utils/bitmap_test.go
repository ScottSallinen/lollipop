package utils

import (
	"math/rand"
	"reflect"
	"testing"
)

func fillBitmapSet(toFill []uint32) Bitmap {
	var bm Bitmap
	for _, j := range toFill {
		bm.Set(j)
	}
	return bm
}

func fillBitmapQuickSet(toFill []uint32) Bitmap {
	var bm Bitmap
	for _, j := range toFill {
		if !bm.QuickSet(j) {
			bm.Set(j)
		}
	}
	return bm
}

const bm_test_size = 32

func Benchmark_BitmapNew(b *testing.B) {
	entries := make([]uint32, bm_test_size)
	for i := 0; i < bm_test_size; i++ {
		entries[i] = rand.Uint32() % bm_test_size
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var bm Bitmap // New bitmap
		for _, j := range entries {
			if !bm.QuickSet(j) {
				bm.Set(j)
			}
		}
		bm.FirstUnused()
		bm.Zeroes()
	}
}

func Benchmark_BitmapReg(b *testing.B) {
	entries := make([]uint32, bm_test_size)
	for i := 0; i < bm_test_size; i++ {
		entries[i] = rand.Uint32() % bm_test_size
	}

	b.ResetTimer()

	var bm Bitmap // Re-use bitmap
	bm.Grow(bm_test_size - 1)

	for i := 0; i < b.N; i++ {
		for _, j := range entries {
			if !bm.QuickSet(j) {
				bm.Set(j)
			}
		}
		bm.FirstUnused()
		bm.Zeroes()
	}
}

func Benchmark_BitmapRegSet(b *testing.B) {
	entries := make([]uint32, bm_test_size)
	for i := 0; i < bm_test_size; i++ {
		entries[i] = rand.Uint32() % bm_test_size
	}

	var bm Bitmap

	b.ResetTimer()
	bm.Grow(bm_test_size - 1)

	for i := 0; i < b.N; i++ {
		for _, j := range entries {
			bm.Set(j)
		}
		bm.FirstUnused()
		bm.Zeroes()
	}
}

func Test_FindFirstUnused(t *testing.T) {
	nbrsTests := [][]uint32{
		{},
		{0},
		{1},
		{0, 1},
		{1, 0},
		{0, 2},
		{0, 1, 2, 3},
		{1, 2, 3},
		{2, 4, 1, 0},
		{12, 0, 2, 2, 2, 3, 0, 1},
		{7, 4, 0, 2, 2, 5, 3, 0, 1, 5, 8},
	}
	// Sets two uints to all set.
	nbrsTests = append(nbrsTests, make([]uint32, 64))
	for i := 0; i < 64; i++ {
		nbrsTests[len(nbrsTests)-1][i] = uint32(i)
	}
	// Sets two uints to all set, plus one in the next uint.
	nbrsTests = append(nbrsTests, make([]uint32, 65))
	for i := 0; i < 65; i++ {
		nbrsTests[len(nbrsTests)-1][i] = uint32(i)
	}
	// Sets three uints to all set.
	nbrsTests = append(nbrsTests, make([]uint32, 128))
	for i := 0; i < 128; i++ {
		nbrsTests[len(nbrsTests)-1][i] = uint32(i)
	}
	// Sets three uints to all set, plus one in the next uint.
	nbrsTests = append(nbrsTests, make([]uint32, 129))
	for i := 0; i < 129; i++ {
		nbrsTests[len(nbrsTests)-1][i] = uint32(i)
	}
	nbrsTestsAns := []uint32{
		0,
		1,
		0,
		2,
		2,
		1,
		4,
		0,
		3,
		4,
		6,
		64,
		65,
		128,
		129,
	}

	for test := range nbrsTests {
		assertEqual(t, nbrsTestsAns[test], fillBitmapSet(nbrsTests[test]).FirstUnused(), F("%d", test))
	}
	for test := range nbrsTests {
		assertEqual(t, nbrsTestsAns[test], fillBitmapQuickSet(nbrsTests[test]).FirstUnused(), F("%d", test))
	}
}

func assertEqual(_ *testing.T, expected any, actual any, prefix string) {
	if reflect.DeepEqual(expected, actual) {
		return
	}
	str := prefix + ": Expected: " + V(expected) + "; != given: " + V(actual)
	panic(str)
}
