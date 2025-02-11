package utils

type PQI[T any] interface {
	Less(T) bool
}

type PQ[T PQI[T]] []T

// Init establishes the heap invariants required by the other routines in this package.
// Init is idempotent with respect to the heap invariants
// and may be called whenever the heap invariants may have been invalidated.
// The complexity is O(n) where n = h.Len().
func (h PQ[T]) Init() {
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func (h *PQ[T]) Push(x T) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
func (h *PQ[T]) Pop() T {
	n := len(*h) - 1
	(*h)[0], (*h)[n] = (*h)[n], (*h)[0]
	h.down(0, n)
	old := h
	nt := len(*old)
	item := (*old)[nt-1]
	*h = (*old)[0 : nt-1]
	return item
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
// The complexity is O(log n) where n = h.Len().
func (h PQ[T]) Fix(i int) {
	if !h.down(i, len(h)) {
		h.up(i)
	}
}

func (h PQ[T]) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h[j].Less(h[i]) {
			break
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func (h PQ[T]) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h[j2].Less(h[j1]) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h[j].Less(h[i]) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
	return i > i0
}

// BinarySearchIdxFunc is like BinarySearchFunc, but provides the index into X rather than a value copy.
// This is useful because Go is a silly language that does not have std::distance and cannot reverse understand position index.
//
// BinarySearchFunc works like [BinarySearch], but uses a custom comparison
// function. The slice must be sorted in increasing order, where "increasing"
// is defined by cmp. cmp should return 0 if the slice element matches
// the target, a negative number if the slice element precedes the target,
// or a positive number if the slice element follows the target.
// cmp must implement the same ordering as the slice, such that if
// cmp(a, t) < 0 and cmp(b, t) >= 0, then a must precede b in the slice.
func BinarySearchIdxFunc[S ~[]E, E, T any](x S, target T, cmp func(int, T) int) (int, bool) {
	n := len(x)
	// Define cmp(x[-1], target) < 0 and cmp(x[n], target) >= 0 .
	// Invariant: cmp(x[i - 1], target) < 0, cmp(x[j], target) >= 0.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if cmp(h, target) < 0 {
			i = h + 1 // preserves cmp(x[i - 1], target) < 0
		} else {
			j = h // preserves cmp(x[j], target) >= 0
		}
	}
	// i == j, cmp(x[i-1], target) < 0, and cmp(x[j], target) (= cmp(x[i], target)) >= 0  =>  answer is i.
	return i, i < n && cmp(i, target) == 0
}
