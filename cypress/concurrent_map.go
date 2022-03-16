package cypress

import (
	"sync"
)

// ConcurrentMap a concurrent map
type ConcurrentMap[TKey comparable, TValue any] struct {
	lock   *sync.RWMutex
	values map[TKey]TValue
}

// NewConcurrentMap creates a new instance of ConcurrentMap
func NewConcurrentMap[TKey comparable, TValue any]() *ConcurrentMap[TKey, TValue] {
	return &ConcurrentMap[TKey, TValue]{&sync.RWMutex{}, make(map[TKey]TValue)}
}

// Put puts a value to the map associate to the map and return the old value
func (m *ConcurrentMap[TKey, TValue]) Put(key TKey, value TValue) (TValue, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	oldValue, ok := m.values[key]
	m.values[key] = value
	return oldValue, ok
}

// Foreach iterates the map and passes the key and value to the given function
func (m *ConcurrentMap[TKey, TValue]) Foreach(f func(key TKey, value TValue)) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for k, v := range m.values {
		f(k, v)
	}
}

// RemoveIf iterates the map and delete all items that the evaluator returns true
// returns number of items that were removed
func (m *ConcurrentMap[TKey, TValue]) RemoveIf(evaluator func(key TKey, value TValue) bool) int {
	m.lock.Lock()
	defer m.lock.Unlock()
	keysToRemove := make([]TKey, 0)
	for k, v := range m.values {
		if evaluator(k, v) {
			keysToRemove = append(keysToRemove, k)
		}
	}

	for _, k := range keysToRemove {
		delete(m.values, k)
	}

	return len(keysToRemove)
}

// Delete deletes the specified key from the map
func (m *ConcurrentMap[TKey, TValue]) Delete(key TKey) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.values, key)
}

// Get gets a value for the given key if it exists
func (m *ConcurrentMap[TKey, TValue]) Get(key TKey) (TValue, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	value, ok := m.values[key]
	return value, ok
}

// Len gets the length of the underlying values
func (m *ConcurrentMap[TKey, TValue]) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.values)
}

// GetOrCompute gets a value from map if it does not exist
// compute the value from the given generator
func (m *ConcurrentMap[TKey, TValue]) GetOrCompute(key TKey, generator func() TValue) TValue {
	var value TValue
	var ok bool
	func() {
		m.lock.RLock()
		defer m.lock.RUnlock()
		value, ok = m.values[key]
	}()

	if ok {
		return value
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	value, ok = m.values[key]
	if !ok {
		value = generator()
		m.values[key] = value
	}

	return value
}
