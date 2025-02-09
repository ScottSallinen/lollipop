package main

import "sync"

type ConcurrentMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		mu: sync.RWMutex{},
		m:  make(map[K]V),
	}
}

func NewConcurrentMapFromMap[K comparable, V any](prev map[K]V) *ConcurrentMap[K, V] {
	newMap := make(map[K]V)
	for k, v := range prev {
		newMap[k] = v
	}
	return &ConcurrentMap[K, V]{
		mu: sync.RWMutex{},
		m:  newMap,
	}
}

// Set adds or updates a key-value pair
func (c *ConcurrentMap[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = value
}

// Get retrieves a value safely
func (c *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, exists := c.m[key]
	return val, exists
}

func (c *ConcurrentMap[K, V]) Lock() {
	c.mu.Lock()
}

func (c *ConcurrentMap[K, V]) Unlock() {
	c.mu.Unlock()
}

func (c *ConcurrentMap[K, V]) SetNoLock(key K, value V) {
	c.m[key] = value
}

// Get retrieves a value safely
func (c *ConcurrentMap[K, V]) GetNoLock(key K) (V, bool) {
	val, exists := c.m[key]
	return val, exists
}

// Delete removes a key from the map
func (c *ConcurrentMap[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, key)
}

// Size returns the number of elements in the map
func (c *ConcurrentMap[K, V]) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

func (c *ConcurrentMap[K, V]) Content() map[K]V {
	copyMap := make(map[K]V)
	c.mu.RLock()
	for k, v := range c.m {
		copyMap[k] = v
	}
	c.mu.RUnlock()
	return copyMap
}

func (c *ConcurrentMap[K, V]) Clear() {
	c.mu.Lock()
	c.m = make(map[K]V)
	c.mu.Unlock()
}
