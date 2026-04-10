package protodb

import (
	"os"
	"sync"
)

type FileTable struct {
	mu  sync.Mutex
	lru *LRU[string, *os.File]
}

func (ft *FileTable) getOrOpen(path string) (*os.File, error) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	handle, ok := ft.lru.Get(path)
	if ok {
		return handle, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	ft.lru.Put(path, file)
	return file, nil
}

func (ft *FileTable) Read(path string, offset int64, length int64) ([]byte, error) {
	handle, err := ft.getOrOpen(path)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, length)
	if _, err := handle.ReadAt(buffer, offset); err != nil {
		return nil, err
	}
	return buffer, nil
}

func (ft *FileTable) Close(path string) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.lru.Remove(path)
}

func (ft *FileTable) Clear() {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.lru.Clear()
}

func newFileTable(capacity int) *FileTable {
	return &FileTable{
		lru: newLRU(capacity, func(path string, file *os.File) {
			file.Close()
		}),
	}
}
