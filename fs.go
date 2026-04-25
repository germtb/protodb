package protodb

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

// FS is protodb's minimal filesystem abstraction. The engine performs every
// filesystem op through this interface, letting tests inject error-injecting
// or in-memory implementations without a dependency on any external VFS.
//
// Signatures are deliberately narrow — a single `name` parameter per method,
// no tagged categories — so that adapter types in sibling test modules can
// wrap richer interfaces (Pebble's vfs.FS, fstest, etc.) with small shims.
type FS interface {
	// Open opens a file for reading.
	Open(name string) (File, error)
	// OpenAppend opens (or creates) a file for append-only writing.
	// Used for WAL and manifest.
	OpenAppend(name string) (File, error)
	// Create creates or truncates a file for writing.
	Create(name string) (File, error)
	// OpenDir opens a directory as a File — used only to Sync() the directory
	// so that preceding creates/renames are durable.
	OpenDir(name string) (File, error)
	Remove(name string) error
	Rename(oldname, newname string) error
	MkdirAll(name string, perm os.FileMode) error
	Truncate(name string, size int64) error
	ReadFile(name string) ([]byte, error)
	// List returns the names of entries directly inside dir. No recursion,
	// no full DirEntry — protodb only needs filenames.
	List(dir string) ([]string, error)
	Stat(name string) (FileInfo, error)
}

// File is the subset of *os.File behaviour that protodb actually uses. It is
// a strict subset of Pebble's vfs.File, so any Pebble file value can flow
// directly through this interface without adaptation. Stat is intentionally
// omitted — file size is obtained via FS.Stat(name) instead, which avoids
// Go's strict method-return-type identity requirement (os.FileInfo vs our
// FileInfo) and keeps File's cross-implementation structural match trivial.
// Truncate/Seek are likewise excluded; callers use FS.Truncate(name, size)
// and close/reopen instead, which Pebble's vfs.File does support.
type File interface {
	io.ReaderAt
	io.Writer
	io.Closer
	Sync() error
}

// FileInfo is the subset of os.FileInfo protodb uses (just Size). Both
// os.FileInfo and Pebble's vfs.FileInfo satisfy this structurally.
type FileInfo interface {
	Size() int64
}

// DefaultFS is the os-backed filesystem used when Open() is called without
// a WithFS option.
var DefaultFS FS = osFS{}

// osFS is the default, real-disk implementation backed by the stdlib.
type osFS struct{}

func (osFS) Open(name string) (File, error) { return os.Open(name) }

func (osFS) OpenAppend(name string) (File, error) {
	return os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

func (osFS) Create(name string) (File, error) { return os.Create(name) }

func (osFS) OpenDir(name string) (File, error) { return os.Open(name) }

func (osFS) Remove(name string) error { return os.Remove(name) }

func (osFS) Rename(oldname, newname string) error { return os.Rename(oldname, newname) }

func (osFS) MkdirAll(name string, perm os.FileMode) error { return os.MkdirAll(name, perm) }

func (osFS) Truncate(name string, size int64) error { return os.Truncate(name, size) }

func (osFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

func (osFS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out, nil
}

func (osFS) Stat(name string) (FileInfo, error) { return os.Stat(name) }

// tempCounter produces unique suffixes for CreateTempFile. Scoped to the
// process; combined with a nanosecond timestamp in the name it's unique
// across processes too.
var tempCounter atomic.Uint64

// CreateTempFile creates a file in dir with a unique name derived from
// prefix + counter + PID. Replaces os.CreateTemp for FS-backed paths.
func CreateTempFile(fs FS, dir, prefix string) (File, string, error) {
	name := filepath.Join(dir, prefix+strconv.FormatInt(int64(os.Getpid()), 36)+
		"-"+strconv.FormatUint(tempCounter.Add(1), 36))
	f, err := fs.Create(name)
	if err != nil {
		return nil, "", err
	}
	return f, name, nil
}
