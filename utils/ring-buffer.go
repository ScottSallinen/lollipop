package utils

import (
	"sync/atomic"

	_ "math"
)

// Enqueuer : Producer
// Dequeuer : Consumer
// SP : Single Producer
// MP : Multiple Producers
// SC : Single Consumer
// MC : Multiple Consumers (not implemented)

// ---------------------------- SPSC Ring Buffer ----------------------------

// Maybe 30% faster than the MPSC version? Depends on usage.
// But much worse if hammering accepts.
// Better memory space efficiency.
type RingBuffSPSC[T any] struct {
	_           [0]atomic.Int64
	enqueue     uint64
	enqDeqCache uint64
	enqMask     uint64
	enqEntries  []T
	_           [2]uint64
	dequeue     uint64
	deqEnqCache uint64
	deqMask     uint64
	deqEntries  []T
	status      uint64
	_           [1]uint64
}

// Will allocate and initialize the ring buffer with the specified size.
func (rb *RingBuffSPSC[T]) Init(size uint64) {
	size = RoundUpPow(size)
	rb.enqMask = (size - 1)
	rb.deqMask = rb.enqMask
	rb.enqEntries = make([]T, size)
	rb.deqEntries = rb.enqEntries
}

// Should be called by the enqueuer.
func (rb *RingBuffSPSC[T]) Close() {
	atomic.StoreUint64(&rb.status, 1)
}

// To be called by dequeuer after after it sees close (and has dequeued everything).
func (rb *RingBuffSPSC[T]) End() {
	rb.enqEntries = nil
	rb.deqEntries = nil
}

// Returns the total capacity of the ring buffer. Call this if you are the enqueuer (to avoid loading the dequeuer cache line).
func (rb *RingBuffSPSC[T]) EnqCap() uint64 {
	return rb.enqMask + 1
}

// Returns the total capacity of the ring buffer. Call this if you are the dequeuer (to avoid loading the enqueuer cache line).
func (rb *RingBuffSPSC[T]) DeqCap() uint64 {
	return rb.deqMask + 1
}

// How many spots are currently available to enqueue.
// Will update the enqDeqCache for the enqueuer (this call loads the dequeuer's cache line).
func (rb *RingBuffSPSC[T]) EnqCheckRange() uint64 {
	rb.enqDeqCache = atomic.LoadUint64(&rb.dequeue)
	return (rb.enqDeqCache + rb.enqMask) - rb.enqueue
}

// How many elements are currently available to dequeue.
// Will update the deqEnqCache for the dequeuer (this call loads the enqueuer's cache line).
func (rb *RingBuffSPSC[T]) DeqCheckRange() uint64 {
	rb.deqEnqCache = atomic.LoadUint64(&rb.enqueue)
	return rb.deqEnqCache - rb.dequeue
}

// Dequeuer: Return the next item, or false and the desired position in the buffer if empty.
func (rb *RingBuffSPSC[T]) Accept() (item T, ok bool) {
	pos := rb.dequeue
	enqPos := rb.deqEnqCache
	if pos >= enqPos {
		enqPos = atomic.LoadUint64(&rb.enqueue)
		rb.deqEnqCache = enqPos
	}
	if pos < enqPos {
		item = rb.deqEntries[pos&rb.deqMask]
		atomic.StoreUint64(&rb.dequeue, pos+1)
		return item, true
	}
	return item, false
}

// Dequeuer: Blocking get of the item part 1.
func (rb *RingBuffSPSC[T]) GetFast() (item T, ok bool, pos uint64) {
	pos = rb.dequeue
	enqPos := rb.deqEnqCache
	if pos >= enqPos {
		enqPos = atomic.LoadUint64(&rb.enqueue)
		rb.deqEnqCache = enqPos
	}
	if pos < enqPos {
		item = rb.deqEntries[pos&rb.deqMask]
		atomic.StoreUint64(&rb.dequeue, pos+1)
		return item, true, pos
	}
	return item, false, pos
}

// Dequeuer: Blocking get of the item part 2, from the position (from GetFast). Blocks until retrieved.
// Returns the number of times the attempt had to be retried (if any), or if the buffer becomes closed.
func (rb *RingBuffSPSC[T]) GetSlow(pos uint64) (item T, closed bool, fails int) {
	n := &rb.deqEntries[pos&rb.deqMask]
	enqPos := atomic.LoadUint64(&rb.enqueue)
	for ; ; fails++ {
		if pos < enqPos {
			rb.deqEnqCache = enqPos
			item = *n
			atomic.StoreUint64(&rb.dequeue, pos+1)
			return item, false, fails
		}
		if atomic.LoadUint64(&rb.status) == 1 {
			return item, true, fails
		}
		BackOff(fails) // Empty
		enqPos = atomic.LoadUint64(&rb.enqueue)
	}
}

// Enqueuer: Offers the item. Returns false if there is no space, giving the desired position in the buffer.
func (rb *RingBuffSPSC[T]) Offer(item T) (ok bool) {
	pos := rb.enqueue
	deqPos := rb.enqDeqCache
	if pos > (deqPos + rb.enqMask) {
		deqPos = atomic.LoadUint64(&rb.dequeue)
		rb.enqDeqCache = deqPos
	}
	if pos <= (deqPos + rb.enqMask) {
		rb.enqEntries[pos&rb.enqMask] = item
		atomic.StoreUint64(&rb.enqueue, pos+1)
		return true
	}
	return false
}

// Enqueuer: Offers the item. Returns false if there is no space, giving the desired position in the buffer.
func (rb *RingBuffSPSC[T]) PutFast(item T) (pos uint64, ok bool) {
	pos = rb.enqueue
	deqPos := rb.enqDeqCache
	if pos > (deqPos + rb.enqMask) {
		deqPos = atomic.LoadUint64(&rb.dequeue)
		rb.enqDeqCache = deqPos
	}
	if pos <= (deqPos + rb.enqMask) {
		rb.enqEntries[pos&rb.enqMask] = item
		atomic.StoreUint64(&rb.enqueue, pos+1)
		return pos, true
	}
	return pos, false
}

// Enqueuer: Adds the item. Blocks until added.
// Call after Offer fails, giving the desired position in the buffer.
func (rb *RingBuffSPSC[T]) PutSlow(item T, pos uint64) (fails int) {
	n := &rb.enqEntries[pos&rb.enqMask]
	deqPos := atomic.LoadUint64(&rb.dequeue)
	for ; ; fails++ {
		if pos <= (deqPos + rb.enqMask) {
			rb.enqDeqCache = deqPos
			*n = item
			atomic.StoreUint64(&rb.enqueue, pos+1)
			return fails
		}
		BackOff(fails) // Full
		deqPos = atomic.LoadUint64(&rb.dequeue)
	}
}

// ---------------------------- (SP|MP)SC Ring Buffer ----------------------------

// MP or SP, but make sure you use it with the right calls (i.e., only use SP calls if you know you are the only possible producer at the time).
// This is a bit slower than the SPSC version.
type RingBuffMPSC[T any] struct {
	_          [0]atomic.Int64
	enqueue    uint64
	enqMask    uint64
	enqEntries []PosElement[T]
	_          [3]uint64
	dequeue    uint64
	deqMask    uint64
	deqEntries []PosElement[T]
	status     uint64
	_          [2]uint64
}

type PosElement[T any] struct {
	position uint64
	element  T
}

// Will allocate and initialize the ring buffer with the specified size.
func (rb *RingBuffMPSC[T]) Init(size uint64) {
	size = RoundUpPow(size)
	rb.enqMask = (size - 1)
	rb.deqMask = rb.enqMask
	rb.deqEntries = make([]PosElement[T], size)
	for i := 0; i < int(size); i++ {
		rb.deqEntries[i].position = uint64(i)
	}
	rb.enqEntries = rb.deqEntries
}

// Returns the total capacity of the ring buffer. Call this if you are the dequeuer (to avoid loading the enqueuer cache line).
func (rb *RingBuffMPSC[T]) DeqCap() uint64 {
	return rb.deqMask + 1
}

// Returns the total capacity of the ring buffer. Call this if you are the enqueuer (to avoid loading the dequeuer cache line).
func (rb *RingBuffMPSC[T]) EnqCap() uint64 {
	return rb.enqMask + 1
}

// Might not be accurate if there are concurrent accesses. Should only be used for an estimate.
// Loads both cache lines!
func (rb *RingBuffMPSC[T]) Len() uint64 {
	return atomic.LoadUint64(&rb.enqueue) - atomic.LoadUint64(&rb.dequeue)
}

// Should be called by the enqueuer.
func (rb *RingBuffMPSC[T]) Close() {
	atomic.StoreUint64(&rb.status, 1)
}

// To be called by dequeuer after after it sees close (and has dequeued everything).
func (rb *RingBuffMPSC[T]) End() {
	rb.enqEntries = nil
	rb.deqEntries = nil
}

// Dequeuer: How many elements available to dequeue
func (rb *RingBuffMPSC[T]) DeqCheckRange() uint64 {
	return atomic.LoadUint64(&rb.enqueue) - rb.dequeue
}

// (SingleProducer) Enqueuer: Offers the item. Just returns false if no item available.
func (rb *RingBuffMPSC[T]) OfferSP(item T) bool {
	pos := rb.enqueue
	n := &rb.enqEntries[pos&rb.enqMask]
	if atomic.LoadUint64(&n.position) == pos {
		n.element = item
		rb.enqueue++
		atomic.StoreUint64(&n.position, pos+1)
		return true
	}
	return false
}

// (SingleProducer) Enqueuer: Blocking add of the item part 1, MOVES FORWARD, must call PutSlowSP if !ok.
func (rb *RingBuffMPSC[T]) PutFastSP(item T) (pos uint64, ok bool) {
	pos = rb.enqueue
	rb.enqueue++
	n := &rb.enqEntries[pos&rb.enqMask]
	if atomic.LoadUint64(&n.position) == pos {
		n.element = item
		atomic.StoreUint64(&n.position, pos+1)
		return pos, true
	}
	return pos, false
}

// (SingleProducer) Enqueuer: Blocking add of the item part 2, to the position (from PutFastSP). Blocks until added.
func (rb *RingBuffMPSC[T]) PutSlowSP(item T, pos uint64) (fails int) {
	n := &rb.enqEntries[pos&rb.enqMask]
	for ; ; fails++ {
		if atomic.LoadUint64(&n.position) == pos {
			n.element = item
			atomic.StoreUint64(&n.position, pos+1)
			return
		}
		BackOff(fails) // Full
	}
}

// Dequeuer: Return the next item, or false if empty.
func (rb *RingBuffMPSC[T]) Accept() (item T, ok bool) {
	pos := rb.dequeue
	n := &rb.deqEntries[pos&rb.deqMask]
	if atomic.LoadUint64(&n.position) == (pos + 1) {
		item = n.element
		rb.dequeue++
		atomic.StoreUint64(&n.position, (pos + 1 + rb.deqMask))
		return item, true
	}
	return item, false
}

// Dequeuer: Blocking get of the item part 1, MOVES FORWARD, must call GetSlowSC if !ok.
func (rb *RingBuffMPSC[T]) GetFast() (item T, ok bool, pos uint64) {
	n := &rb.deqEntries[rb.dequeue&rb.deqMask]
	rb.dequeue++
	pos = rb.dequeue
	if atomic.LoadUint64(&n.position) == pos {
		item = n.element
		atomic.StoreUint64(&n.position, (pos + rb.deqMask))
		return item, true, pos
	}
	return item, false, pos
}

// Dequeuer: Blocking get of the item part 2, from the position (from GetFast). Blocks until retrieved.
func (rb *RingBuffMPSC[T]) GetSlow(pos uint64) (item T, closed bool, fails int) {
	n := &rb.deqEntries[(pos-1)&rb.deqMask]
	for ; ; fails++ {
		if atomic.LoadUint64(&n.position) == pos {
			item = n.element
			atomic.StoreUint64(&n.position, (pos + rb.deqMask))
			return item, false, fails
		}
		if atomic.LoadUint64(&rb.status) == 1 {
			return item, true, fails
		}
		BackOff(fails) // Empty
	}
}

// (MultipleProducers) Enqueuer: Offer item. Return false on failure. Does not advance position.
func (rb *RingBuffMPSC[T]) OfferMP(item T) (ok bool) {
	pos := atomic.LoadUint64(&rb.enqueue)
	n := &rb.enqEntries[pos&rb.enqMask]
	if atomic.LoadUint64(&n.position) == pos {
		if atomic.CompareAndSwapUint64(&rb.enqueue, pos, pos+1) {
			n.element = item
			atomic.StoreUint64(&n.position, pos+1)
			return true
		}
	}
	return false
}

// (MultipleProducers) Enqueuer: Blocking add of the item part 1, MOVES FORWARD, must call PutSlowMP if !ok.
func (rb *RingBuffMPSC[T]) PutFastMP(item T) (myPos uint64, ok bool) {
	myPos = atomic.AddUint64(&rb.enqueue, 1) - 1
	n := &rb.enqEntries[myPos&rb.enqMask]
	if atomic.LoadUint64(&n.position) == myPos {
		n.element = item
		atomic.StoreUint64(&n.position, myPos+1)
		return myPos, true
	}
	return myPos, false
}

// (MultipleProducers) Enqueuer: Blocking add of the item part 2, to the position (from PutFastMP). Blocks until added.
func (rb *RingBuffMPSC[T]) PutSlowMP(item T, myPos uint64) (fails int) {
	n := &rb.enqEntries[myPos&rb.enqMask]
	for ; ; fails++ {
		if atomic.LoadUint64(&n.position) == myPos {
			n.element = item
			atomic.StoreUint64(&n.position, myPos+1)
			return
		}
		BackOff(fails) // Full
	}
}

// ---------------------------- SP, SC Growable Ring Buffer ----------------------------

// SPSC, but can grow if the buffer is full.
// Note the growth strategy is not unbounded and does not prevent blocking.
// The growth strategy is to double the size of the buffer, but it cannot grow a second time,
// until at least the consumer has caught up and acknowledged the first growth.
// In other words, this is more to perform a sort of back-pressure strategy that implies there
// are big chunks of work / bursts to be done, and we want/need a bigger space to enqueue all at once.
type GrowableRingBuff[T any] struct {
	_          [0]atomic.Int64
	enqueue    uint64
	enqMask    uint64
	enqEntries []PosElement[T]
	MaxGrow    uint64
	_          [2]uint64
	dequeue    uint64
	deqMask    uint64
	deqEntries []PosElement[T]
	status     uint64
	isGrown    uint64
	_          [1]uint64
}

// Will allocate and initialize the ring buffer with the specified size.
func (rb *GrowableRingBuff[T]) Init(size uint64, maxGrowTimes uint64) {
	size = RoundUpPow(size)
	rb.deqMask = (size - 1)
	rb.enqMask = rb.deqMask
	rb.MaxGrow = maxGrowTimes
	rb.deqEntries = make([]PosElement[T], size)

	for i := 0; i < int(size); i++ {
		rb.deqEntries[i].position = uint64(i)
	}
	rb.enqEntries = rb.deqEntries
}

// Returns the total capacity of the ring buffer. Call this if you are the dequeuer (to avoid loading the enqueuer cache line).
func (rb *GrowableRingBuff[T]) DeqCap() uint64 {
	return rb.enqMask + 1
}

// Returns the total capacity of the ring buffer. Call this if you are the enqueuer (to avoid loading the dequeuer cache line).
func (rb *GrowableRingBuff[T]) EnqCap() uint64 {
	return rb.enqMask + 1
}

// To be called by the enqueuer.
func (rb *GrowableRingBuff[T]) Close() {
	atomic.StoreUint64(&rb.status, 1)
}

func (rb *GrowableRingBuff[T]) IsClosed() bool {
	if atomic.LoadUint64(&rb.isGrown) == 0 && atomic.LoadUint64(&rb.status) == 1 {
		return atomic.LoadUint64(&rb.enqueue) == atomic.LoadUint64(&rb.dequeue)
	}
	return false
}

// To be called by dequeuer after after it sees close (and has dequeued everything).
func (rb *GrowableRingBuff[T]) End() {
	rb.deqEntries = nil
	rb.enqEntries = nil
}

// Enqueuer operation
func (rb *GrowableRingBuff[T]) growWithStart(startPos uint64, item T) {
	newSize := uint64(len(rb.enqEntries) * 2)
	rb.enqEntries = make([]PosElement[T], newSize)
	rb.enqMask = newSize - 1

	n := &rb.enqEntries[startPos&rb.enqMask]
	n.element = item
	n.position = startPos + 1

	for i := uint64(1); i < newSize; i++ {
		pos := ((startPos + i) & rb.enqMask)
		rb.enqEntries[pos] = PosElement[T]{position: startPos + i}
	}

	rb.MaxGrow -= 1
	atomic.StoreUint64(&rb.isGrown, 1)
}

// Enqueuer operation
func (rb *GrowableRingBuff[T]) CanGrow() bool {
	return (rb.MaxGrow != 0) && (atomic.LoadUint64(&rb.isGrown) == 0)
}

// Enqueuer: adds the item, if there is space. Returns true if added.
func (rb *GrowableRingBuff[T]) Offer(item T) (ok bool) {
	pos := rb.enqueue
	n := &rb.enqEntries[pos&rb.enqMask]
	if atomic.LoadUint64(&n.position) == pos {
		n.element = item
		rb.enqueue++
		atomic.StoreUint64(&n.position, pos+1)
		return true
	}
	return false
}

// Enqueuer: adds the item, if there is space. Returns true if added, and the position in the buffer (can call blocking Put with this).
func (rb *GrowableRingBuff[T]) PutFast(item T) (pos uint64, ok bool) {
	pos = rb.enqueue
	rb.enqueue++
	n := &rb.enqEntries[pos&rb.enqMask]
	if atomic.LoadUint64(&n.position) == pos {
		n.element = item
		atomic.StoreUint64(&n.position, pos+1)
		return pos, true
	}
	return pos, false
}

// Enqueuer: adds the item. Blocks until added. Returns the number of times the attempt had to be retried (if any).
func (rb *GrowableRingBuff[T]) PutSlow(item T, pos uint64) (fails int) {
	n := &rb.enqEntries[pos&rb.enqMask]
	for ; ; fails++ {
		if atomic.LoadUint64(&n.position) == pos {
			n.element = item
			atomic.StoreUint64(&n.position, pos+1)
			return fails
		}
		if rb.CanGrow() {
			rb.growWithStart(pos, item)
			return fails
		}
		BackOff(fails) // Full
	}
}

// Dequeuer: return the next item, or false on failure; Note failure may not mean no items left -- as the buffer may have grown.
func (rb *GrowableRingBuff[T]) Accept() (item T, ok bool) {
	pos := rb.dequeue
	n := &rb.deqEntries[pos&rb.deqMask]
	if atomic.LoadUint64(&n.position) == (pos + 1) {
		item = n.element
		rb.dequeue++
		atomic.StoreUint64(&n.position, (pos + 1 + rb.deqMask))
		return item, true
	}
	if atomic.LoadUint64(&rb.isGrown) == 1 { // We have grown, so we need to switch to the new buffer. This will take effect next Accept. (not enough room to inline)
		rb.deqMask = rb.enqMask
		rb.deqEntries = rb.enqEntries
		atomic.StoreUint64(&rb.isGrown, 0)
	}
	return item, false
}

// Dequeuer: Blocking get of the item part 1, MOVES FORWARD, must call GetSlow if !ok.
func (rb *GrowableRingBuff[T]) GetFast() (item T, ok bool, pos uint64) {
	n := &rb.deqEntries[rb.dequeue&rb.deqMask]
	rb.dequeue++
	pos = rb.dequeue
	if atomic.LoadUint64(&n.position) == pos {
		item = n.element
		atomic.StoreUint64(&n.position, (pos + rb.deqMask))
		return item, true, pos
	}
	if atomic.LoadUint64(&rb.isGrown) == 1 { // We have grown, so we need to switch to the new buffer.
		rb.deqMask = rb.enqMask
		rb.deqEntries = rb.enqEntries
		atomic.StoreUint64(&rb.isGrown, 0)
	}
	return item, false, pos
}

// Dequeuer: Blocking get of the item part 2, from the position (from GetFast). Blocks until retrieved.
func (rb *GrowableRingBuff[T]) GetSlow(pos uint64) (item T, closed bool, fails int) {
	n := &rb.deqEntries[(pos-1)&rb.deqMask]
	for ; ; fails++ {
		if atomic.LoadUint64(&n.position) == pos {
			item = n.element
			atomic.StoreUint64(&n.position, (pos + rb.deqMask))
			return item, false, fails
		}
		if atomic.LoadUint64(&rb.isGrown) == 1 { // We have grown, so we need to switch to the new buffer.
			rb.deqMask = rb.enqMask
			rb.deqEntries = rb.enqEntries
			atomic.StoreUint64(&rb.isGrown, 0)
			n = &rb.deqEntries[(pos-1)&rb.deqMask]
		} else {
			if atomic.LoadUint64(&rb.status) == 1 && atomic.LoadUint64(&rb.isGrown) == 0 {
				if atomic.LoadUint64(&rb.enqueue) != (pos - 1) {
					continue // Think this is required?
				}
				return item, true, fails
			}
			BackOff(fails) // Empty
		}
	}
}
