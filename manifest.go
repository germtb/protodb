package protodb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

/*

Manifest layout:

A log of per-level snapshots. Each frame is a full replacement of one level's
entry list.

┌──────────┬──────────┬──────────┬───────────┐
│ crc32 u32│ level u8 │ len u32  │ n x entry │
└──────────┴──────────┴──────────┴───────────┘

Entry:
┌─────────────┬───────────────┬────────────┬───────────────┬───────────┐
│ hash[32] u8 │ first_len u32 │ first[] u8 │ last_len u32  │ last[] u8 │
└─────────────┴───────────────┴────────────┴───────────────┴───────────┘

Each entry carries the SST's first and last key so Open doesn't need to read
any data blocks to reconstruct SST metadata.

Flush appends one L0 frame. Compaction appends an L1 frame followed by an L0
frame, then one Sync() commits both. Because writes to a single fd preserve
offset ordering, the worst torn-tail scenario is "L1 frame only, no L0"; that
leaves dupes in L0+L1 that the next compaction cleans up. "L0 without L1"
cannot happen.

On replay: each valid CRC frame replaces that level's entry list. On first bad
CRC / short read, stop and truncate.

*/

const hashSize = 32 // sha256

const (
	levelL0 byte = 0
	levelL1 byte = 1
)

type LevelMetadata struct {
	hash  sstHash
	first Key
	last  Key
}

type Manifest struct {
	fs     FS
	path   string
	handle File
	l0     []LevelMetadata
	l1     []LevelMetadata
}

func newManifest(fs FS, path string) (*Manifest, error) {
	handle, err := fs.OpenAppend(path)
	if err != nil {
		return nil, err
	}

	data, err := fs.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var l0, l1 []LevelMetadata
	offset := 0

	for offset+9 <= len(data) {
		checksum := binary.BigEndian.Uint32(data[offset : offset+4])
		level := data[offset+4]
		entriesLen := binary.BigEndian.Uint32(data[offset+5 : offset+9])

		entries, end, ok := decodeEntries(data, offset+9, int(entriesLen))
		if !ok {
			break
		}
		if crc32.ChecksumIEEE(data[offset+4:end]) != checksum {
			break
		}
		if level != levelL0 && level != levelL1 {
			// unknown level byte — treat as corrupt
			break
		}

		if level == levelL0 {
			l0 = entries
		} else {
			l1 = entries
		}

		offset = end
	}

	if offset < len(data) {
		if err := fs.Truncate(path, int64(offset)); err != nil {
			return nil, err
		}
	}

	return &Manifest{
		fs:     fs,
		path:   path,
		handle: handle,
		l0:     l0,
		l1:     l1,
	}, nil
}

// decodeEntries reads n entries starting at data[start]. Returns the entries,
// the byte offset just past the final entry, and false if the encoding runs
// off the end of data.
func decodeEntries(data []byte, start, n int) ([]LevelMetadata, int, bool) {
	entries := make([]LevelMetadata, n)
	pos := start
	for i := range entries {
		if pos+hashSize+4 > len(data) {
			return nil, 0, false
		}
		var hash sstHash
		copy(hash[:], data[pos:pos+hashSize])
		pos += hashSize

		firstLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+firstLen+4 > len(data) {
			return nil, 0, false
		}
		first := append(Key(nil), data[pos:pos+firstLen]...)
		pos += firstLen

		lastLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+lastLen > len(data) {
			return nil, 0, false
		}
		last := append(Key(nil), data[pos:pos+lastLen]...)
		pos += lastLen

		entries[i] = LevelMetadata{hash: hash, first: first, last: last}
	}
	return entries, pos, true
}

func (m *Manifest) L0() []LevelMetadata {
	return m.l0
}

func (m *Manifest) L1() []LevelMetadata {
	return m.l1
}

// Update appends a frame that replaces `level`'s entry list with `metadata`.
// Does not fsync — caller batches with Sync().
func (m *Manifest) Update(level byte, metadata []LevelMetadata) error {
	if level != levelL0 && level != levelL1 {
		return fmt.Errorf("manifest: invalid level %d", level)
	}

	payloadSize := 0
	for _, e := range metadata {
		payloadSize += hashSize + 4 + len(e.first) + 4 + len(e.last)
	}
	data := make([]byte, 9+payloadSize)
	data[4] = level
	binary.BigEndian.PutUint32(data[5:9], uint32(len(metadata)))

	pos := 9
	for _, e := range metadata {
		copy(data[pos:], e.hash[:])
		pos += hashSize

		binary.BigEndian.PutUint32(data[pos:pos+4], uint32(len(e.first)))
		pos += 4
		copy(data[pos:], e.first)
		pos += len(e.first)

		binary.BigEndian.PutUint32(data[pos:pos+4], uint32(len(e.last)))
		pos += 4
		copy(data[pos:], e.last)
		pos += len(e.last)
	}

	checksum := crc32.ChecksumIEEE(data[4:])
	binary.BigEndian.PutUint32(data[0:4], checksum)

	if _, err := m.handle.Write(data); err != nil {
		return err
	}

	switch level {
	case levelL0:
		m.l0 = metadata
	case levelL1:
		m.l1 = metadata
	}
	return nil
}

func (m *Manifest) Sync() error {
	return m.handle.Sync()
}

// syncDir fsyncs a directory so preceding renames (and the file content they
// reference, via FS metadata-after-data ordering) are durable.
func syncDir(fs FS, path string) error {
	dir, err := fs.OpenDir(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return err
	}
	return dir.Close()
}
