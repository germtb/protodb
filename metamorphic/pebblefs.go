// Package metamorphic adapts Pebble's vfs.FS (and error-injecting variants
// built on top of it, e.g. errorfs) into protodb.FS so we can stress-test
// the engine against adversarial I/O without adding a Pebble dependency to
// the protodb module itself.
package metamorphic

import (
	"io"
	"os"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/germtb/protodb"
)

// PebbleFS wraps a vfs.FS into a protodb.FS. The protodb engine only calls
// the subset of filesystem operations expressed by protodb.FS; this shim
// translates those calls into the richer Pebble API.
type PebbleFS struct {
	Inner vfs.FS
}

// Wrap returns a protodb.FS backed by fs.
func Wrap(fs vfs.FS) protodb.FS {
	return &PebbleFS{Inner: fs}
}

func (p *PebbleFS) Open(name string) (protodb.File, error) {
	f, err := p.Inner.Open(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// OpenAppend opens the file for read/write and positions subsequent writes
// at end-of-file. Pebble's vfs has no native O_APPEND — we use OpenReadWrite
// and track the tail offset ourselves via WriteAt.
func (p *PebbleFS) OpenAppend(name string) (protodb.File, error) {
	info, err := p.Inner.Stat(name)
	switch {
	case err == nil:
		f, err := p.Inner.OpenReadWrite(name)
		if err != nil {
			return nil, err
		}
		return &appendFile{file: f, offset: info.Size()}, nil
	case os.IsNotExist(err):
		f, err := p.Inner.Create(name)
		if err != nil {
			return nil, err
		}
		return &appendFile{file: f, offset: 0}, nil
	default:
		return nil, err
	}
}

func (p *PebbleFS) Create(name string) (protodb.File, error) {
	f, err := p.Inner.Create(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (p *PebbleFS) OpenDir(name string) (protodb.File, error) {
	f, err := p.Inner.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (p *PebbleFS) Remove(name string) error {
	return p.Inner.Remove(name)
}

func (p *PebbleFS) Rename(oldname, newname string) error {
	return p.Inner.Rename(oldname, newname)
}

func (p *PebbleFS) MkdirAll(name string, perm os.FileMode) error {
	return p.Inner.MkdirAll(name, perm)
}

// Truncate resizes a file. Pebble's vfs.FS has no Truncate primitive, so we
// read the existing prefix (if any), recreate the file, and re-write.
func (p *PebbleFS) Truncate(name string, size int64) error {
	var prefix []byte
	if size > 0 {
		existing, err := p.Inner.Open(name)
		if err != nil {
			return err
		}
		prefix = make([]byte, size)
		n, err := existing.ReadAt(prefix, 0)
		_ = existing.Close()
		if err != nil && err != io.EOF {
			return err
		}
		prefix = prefix[:n]
	}
	f, err := p.Inner.Create(name)
	if err != nil {
		return err
	}
	if len(prefix) > 0 {
		if _, err := f.Write(prefix); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

func (p *PebbleFS) ReadFile(name string) ([]byte, error) {
	f, err := p.Inner.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func (p *PebbleFS) List(dir string) ([]string, error) {
	return p.Inner.List(dir)
}

func (p *PebbleFS) Stat(name string) (protodb.FileInfo, error) {
	info, err := p.Inner.Stat(name)
	if err != nil {
		return nil, err
	}
	return info, nil
}


// appendFile wraps a Pebble read/write File to provide append-at-offset
// semantics. It uses WriteAt against a locally-tracked tail offset since
// vfs.File has no Seek.
type appendFile struct {
	file   vfs.File
	offset int64
}

func (a *appendFile) ReadAt(p []byte, off int64) (int, error) {
	return a.file.ReadAt(p, off)
}

func (a *appendFile) Write(p []byte) (int, error) {
	n, err := a.file.WriteAt(p, a.offset)
	a.offset += int64(n)
	return n, err
}

func (a *appendFile) Close() error { return a.file.Close() }

func (a *appendFile) Sync() error { return a.file.Sync() }
