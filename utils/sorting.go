package utils

import (
	"container/heap"
	"sort"

	"golang.org/x/exp/constraints"
)

// Wrapper for sorting that gives the indexes of a hypothetically sorted array.
// It is certainly slower than sorting the array directly, but it does not modify the input array.
type indexed[T constraints.Ordered] struct {
	Index []int
	Input []T
}

func (s indexed[T]) Len() int { return len(s.Index) }
func (s indexed[T]) Swap(i, j int) {
	s.Index[i], s.Index[j] = s.Index[j], s.Index[i]
}

func (s *indexed[T]) Init(input []T, size int) {
	s.Input = input
	s.Index = make([]int, size)
	for i := range s.Index {
		s.Index[i] = i
	}
}

// Smallest first version (adds the less function).
type indexedSf[T constraints.Ordered] struct {
	indexed[T]
}

// Less reports whether the element with index i should sort before the element with index j.
func (s indexedSf[T]) Less(i, j int) bool { return s.Input[s.Index[i]] < s.Input[s.Index[j]] }

// Largest first version (adds the less function).
type indexedLf[T constraints.Ordered] struct {
	indexed[T]
}

// Less reports whether the element with index i should sort before the element with index j.
func (s indexedLf[T]) Less(i, j int) bool { return s.Input[s.Index[i]] > s.Input[s.Index[j]] }

// Does not sort the input array, instead a newly allocated index array that represents the sorted order is returned.
// Smallest values first.
func SortGiveIndexesSmallestFirst[T constraints.Ordered](input []T) []int {
	isf := indexedSf[T]{}
	isf.Init(input, len(input))
	sort.Stable(isf)
	return isf.Index
}

// Does not sort the input array, instead a newly allocated index array that represents the sorted order is returned.
// Largest values first.
func SortGiveIndexesLargestFirst[T constraints.Ordered](input []T) []int {
	ilf := indexedLf[T]{}
	ilf.Init(input, len(input))
	sort.Stable(ilf)
	return ilf.Index
}

// For very small N, this is faster than sorting a large array.
// Does not modify input array.
// Largest values first.
func FindTopNInArray(array []float64, topCount uint32) []Pair[uint32, float64] {
	if topCount > uint32(len(array)) {
		topCount = uint32(len(array))
	}
	// Make a smallest first priority queue, starting with the first N elements.
	pq := PriorityQueueSf[float64]{}
	pq.Init(array, int(topCount))

	// Then iterate over the rest of the array, replacing the smallest element if a larger one is found.
	// It's:
	// Funny, copilot thinks the following "This is O(N) instead of O(N log N) for sorting."
	// It's:
	// Now copilot thinks "Its O(N log N) for the heap, and O(N) for the iteration."
	// Soon, copilot, soon you will be good! ( O(N * C log C) )
	for i := int(topCount); i < len(array); i++ {
		// Replace the smallest element if a larger one is found.
		// We do it this way (smallest first PQ) to track the smallest-of-the-largest to be able to replace it.
		if array[pq.Peek()] < array[i] {
			pq.Replace(0, i)
		}
	}

	topSet := make([]Pair[uint32, float64], topCount)
	for i := uint32(0); i < topCount; i++ {
		index := pq.Extract()
		// Backwards, because the smallest element is at the front (we were tracking smallest-of-the-largest)
		topSet[topCount-i-1] = Pair[uint32, float64]{uint32(index), array[index]}
	}
	return topSet
}

// Smallest first priority queue, that prefers to work on indexes rather than sorting the input.
// Only exception would be inserting a new value, which would be appended to the end of the input array.
// Extract (pop) only removes the index, not the value.
// For convenience, use Init instead of heap.Init, use Insert instead of heap.Push, use Extract instead of heap.Pop,
// and Replace instead of changing then calling heap.Fix.
type PriorityQueueSf[T constraints.Ordered] struct {
	indexedSf[T]
}

func (pq *PriorityQueueSf[T]) Init(input []T, size int) {
	pq.indexedSf.Init(input, size)
	heap.Init(pq)
}

func (pq *PriorityQueueSf[T]) Insert(v T) {
	heap.Push(pq, v)
}

func (pq *PriorityQueueSf[T]) Extract() (v int) {
	return heap.Pop(pq).(int)
}

func (pq *PriorityQueueSf[T]) Peek() (idx int) {
	return (*pq).Index[0]
}

func (pq *PriorityQueueSf[T]) Replace(pos int, idx int) {
	(*pq).Index[pos] = idx
	heap.Fix(pq, pos)
}

// Heap functions below (prefer not to use them directly).

func (pq *PriorityQueueSf[T]) Push(x any) {
	item := x.(T)
	pq.Input = append(pq.Input, item)
	pq.Index = append(pq.Index, len(pq.Index)-1)
}

func (pq *PriorityQueueSf[T]) Pop() any {
	last := len(pq.Index) - 1
	item := pq.Index[last]
	pq.Index = pq.Index[:last]
	return item
}
