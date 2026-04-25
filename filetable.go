package protodb

import (
	"sync"
)

// FileTable is a refcounted LRU of open file handles. Each getOrOpen call
// returns a shared FileHandle and increments its refcount. The caller must
// Close exactly once when done — the fd stays open while refs > 0.
//
// LRU eviction only removes the entry from the lookup map and nils its
// list element; the fd keeps living until the last holder Closes. Callers
// that already hold a handle can keep reading even after eviction.
type FileTable struct {
	mu       sync.Mutex
	fs       FS
	items    map[string]*FileHandle
	head     *FileHandle
	tail     *FileHandle
	capacity int
}

// FileHandle is a shared, refcounted handle to a cached File. It implements
// the `reader` interface. Callers get one via FileTable.getOrOpen and must
// Close it exactly once. Close is NOT idempotent at this layer —
// iterator-level wrappers should guard against double-close with a nil check.
type FileHandle struct {
	file File
	path string
	ft   *FileTable
	refs int // protected by ft.mu
	next *FileHandle
	prev *FileHandle
}

func newFileTable(fs FS, capacity int) *FileTable {
	head := &FileHandle{}
	tail := &FileHandle{}
	head.next = tail
	tail.prev = head
	return &FileTable{
		fs:       fs,
		items:    make(map[string]*FileHandle, capacity),
		head:     head,
		tail:     tail,
		capacity: capacity,
	}
}

func (ft *FileTable) remove(handle *FileHandle) {
	handle.prev.next = handle.next
	handle.next.prev = handle.prev
	handle.prev = nil
	handle.next = nil
}

func (ft *FileTable) prepend(handle *FileHandle) {
	handle.next = ft.head.next
	handle.prev = ft.head
	ft.head.next.prev = handle
	ft.head.next = handle
}

// getOrOpen returns a FileHandle with its refcount incremented by one.
// Caller must Close exactly once.
func (ft *FileTable) getOrOpen(path string) (*FileHandle, error) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	if handle, ok := ft.items[path]; ok {
		handle.refs++
		ft.remove(handle)
		ft.prepend(handle)
		return handle, nil
	}

	file, err := ft.fs.Open(path)
	if err != nil {
		return nil, err
	}
	handle := &FileHandle{file: file, path: path, ft: ft, refs: 1}
	ft.prepend(handle)
	ft.items[path] = handle
	ft.maybeEvict()
	return handle, nil
}

func (handle *FileHandle) ReadAt(p []byte, off int64) (int, error) {
	return handle.file.ReadAt(p, off)
}

// Close releases one ref. If refs hit 0 and the entry was already evicted
// from the LRU, the underlying fd is closed. Must be called exactly once
// per getOrOpen — not idempotent.
func (handle *FileHandle) Close() error {
	handle.ft.mu.Lock()
	defer handle.ft.mu.Unlock()
	handle.refs--
	if handle.refs == 0 && handle.ft.items[handle.path] != handle {
		// This handle was already removed from the items map, so we close it
		// as soon as ref count reaches zero.
		return handle.file.Close()
	}
	return nil
}

func (ft *FileTable) evict(handle *FileHandle) error {
	err := handle.file.Close()
	if err != nil {
		return err
	}
	ft.remove(handle)
	delete(ft.items, handle.path)
	return nil
}

// maybeEvict drops the oldest non-pinned entry from the LRU. Pinned
// entries are skipped — capacity may be temporarily exceeded.
func (ft *FileTable) maybeEvict() {
	spillover := len(ft.items) - ft.capacity
	if spillover <= 0 {
		return
	}

	node := ft.tail.prev

	for spillover > 0 && node != ft.head {
		handle := node
		node = node.prev

		if handle.refs > 0 {
			continue
		}
		// We decide to skip this error so that we can remove as many entries as possible
		err := ft.evict(handle)

		if err != nil {
			spillover -= 1
		}
	}
}

// Clear closes all cached fds unconditionally. Caller must ensure no
// outstanding holders.
func (ft *FileTable) Clear() {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	for _, handle := range ft.items {
		_ = handle.file.Close()
	}
	ft.items = make(map[string]*FileHandle)
	ft.head.next = ft.tail
	ft.tail.prev = ft.head
}
