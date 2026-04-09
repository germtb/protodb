package protodb

import "testing"

func intHeap() Heap[int] {
	return newHeap[int](func(a, b int) bool { return a < b })
}

func TestHeapPushPop(t *testing.T) {
	h := intHeap()
	h.Push(3)
	h.Push(1)
	h.Push(2)

	if h.Pop() != 1 {
		t.Fatal("expected 1")
	}
	if h.Pop() != 2 {
		t.Fatal("expected 2")
	}
	if h.Pop() != 3 {
		t.Fatal("expected 3")
	}
}

func TestHeapPeek(t *testing.T) {
	h := intHeap()
	h.Push(5)
	h.Push(2)
	h.Push(8)

	if h.Peek() != 2 {
		t.Fatalf("Peek: got %d, want 2", h.Peek())
	}
	if h.Len() != 3 {
		t.Fatalf("Len after Peek: got %d, want 3", h.Len())
	}
}

func TestHeapLen(t *testing.T) {
	h := intHeap()
	if h.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", h.Len())
	}
	h.Push(1)
	if h.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", h.Len())
	}
	h.Pop()
	if h.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", h.Len())
	}
}

func TestHeapSingleElement(t *testing.T) {
	h := intHeap()
	h.Push(42)

	if h.Peek() != 42 {
		t.Fatalf("Peek: got %d, want 42", h.Peek())
	}
	if h.Pop() != 42 {
		t.Fatal("expected 42")
	}
	if h.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", h.Len())
	}
}

func TestHeapDuplicates(t *testing.T) {
	h := intHeap()
	h.Push(3)
	h.Push(3)
	h.Push(3)

	for i := range 3 {
		got := h.Pop()
		if got != 3 {
			t.Fatalf("Pop %d: got %d, want 3", i, got)
		}
	}
}

func TestHeapInsertedInOrder(t *testing.T) {
	h := intHeap()
	for i := range 10 {
		h.Push(i)
	}

	for i := range 10 {
		got := h.Pop()
		if got != i {
			t.Fatalf("Pop: got %d, want %d", got, i)
		}
	}
}

func TestHeapInsertedInReverseOrder(t *testing.T) {
	h := intHeap()
	for i := 9; i >= 0; i-- {
		h.Push(i)
	}

	for i := range 10 {
		got := h.Pop()
		if got != i {
			t.Fatalf("Pop: got %d, want %d", got, i)
		}
	}
}

func TestHeapLargeRandom(t *testing.T) {
	h := intHeap()
	// Insert in a scattered order
	values := []int{50, 20, 80, 10, 40, 70, 30, 90, 60, 100}
	for _, v := range values {
		h.Push(v)
	}

	var prev int = 0
	for h.Len() > 0 {
		got := h.Pop()
		if got < prev {
			t.Fatalf("Pop: got %d after %d, not sorted", got, prev)
		}
		prev = got
	}
}

func TestHeapInterleavedPushPop(t *testing.T) {
	h := intHeap()
	h.Push(5)
	h.Push(3)

	if h.Pop() != 3 {
		t.Fatal("expected 3")
	}

	h.Push(1)
	h.Push(4)

	if h.Pop() != 1 {
		t.Fatal("expected 1")
	}
	if h.Pop() != 4 {
		t.Fatal("expected 4")
	}
	if h.Pop() != 5 {
		t.Fatal("expected 5")
	}
}
