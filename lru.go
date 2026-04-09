package protodb

import "container/list"

type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

type LRU[K comparable, V any] struct {
	capacity int
	items    map[K]*list.Element
	order    *list.List
	onEvict  func(key K, value V)
}

func newLRU[K comparable, V any](capacity int, onEvict func(key K, value V)) *LRU[K, V] {
	return &LRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		order:    list.New(),
		onEvict:  onEvict,
	}
}

func (c *LRU[K, V]) Has(key K) bool {
	_, ok := c.items[key]
	return ok
}

func (c *LRU[K, V]) Get(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(lruEntry[K, V]).value, true
}

func (c *LRU[K, V]) Put(key K, value V) {
	if elem, ok := c.items[key]; ok {
		c.order.MoveToFront(elem)
		elem.Value = lruEntry[K, V]{key: key, value: value}
		return
	}

	if c.order.Len() >= c.capacity {
		c.evictOldest()
	}

	elem := c.order.PushFront(lruEntry[K, V]{key: key, value: value})
	c.items[key] = elem
}

func (c *LRU[K, V]) Remove(key K) (V, bool) {
	elem, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	entry := c.order.Remove(elem).(lruEntry[K, V])
	delete(c.items, key)
	if c.onEvict != nil {
		c.onEvict(entry.key, entry.value)
	}
	return entry.value, true
}

func (c *LRU[K, V]) Clear() {
	for c.order.Len() > 0 {
		c.evictOldest()
	}
}

func (c *LRU[K, V]) Len() int {
	return c.order.Len()
}

func (c *LRU[K, V]) evictOldest() {
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := c.order.Remove(back).(lruEntry[K, V])
	delete(c.items, entry.key)
	if c.onEvict != nil {
		c.onEvict(entry.key, entry.value)
	}
}
