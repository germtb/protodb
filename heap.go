package protodb

type Heap[T any] struct {
	items []T
	less  func(a T, b T) bool
}

func newHeap[T any](less func(a T, b T) bool) Heap[T] {
	return Heap[T]{
		items: nil,
		less:  less,
	}
}

func (h *Heap[T]) Len() int {
	return len(h.items)
}

func (h *Heap[T]) Push(item T) {
	h.items = append(h.items, item)
	h.bubbleUp(len(h.items) - 1)
}

func (h *Heap[T]) Pop() T {
	last := len(h.items) - 1
	h.items[0], h.items[last] = h.items[last], h.items[0]
	val := h.items[last]
	h.items = h.items[:last]
	if len(h.items) > 0 {
		h.bubbleDown(0)
	}
	return val
}

func (h *Heap[T]) Peek() T {
	return h.items[0]
}

func (h *Heap[T]) bubbleUp(i int) {
	if i <= 0 {
		return
	}
	parent := (i - 1) / 2
	if h.less(h.items[i], h.items[parent]) {
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		h.bubbleUp(parent)
	}
}

func (h *Heap[T]) bubbleDown(i int) {
	n := len(h.items)
	smallest := i
	left := 2*i + 1
	right := 2*i + 2

	if left < n && h.less(h.items[left], h.items[smallest]) {
		smallest = left
	}
	if right < n && h.less(h.items[right], h.items[smallest]) {
		smallest = right
	}

	if smallest != i {
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		h.bubbleDown(smallest)
	}
}
