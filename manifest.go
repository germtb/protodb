package protodb

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
)

/*

Manifest layout:

A log of per-level snapshots. Each frame is a full replacement of one level's
hash list.

┌──────────┬──────────┬──────────┬──────────────┐
│ crc32 u32│ level u8 │ len u32  │ hash[32]×n   │
└──────────┴──────────┴──────────┴──────────────┘

Flush appends one L0 frame. Compaction appends an L1 frame followed by an L0
frame, then one Sync() commits both. Because writes to a single fd preserve
offset ordering, the worst torn-tail scenario is "L1 frame only, no L0"; that
leaves dupes in L0+L1 that the next compaction cleans up. "L0 without L1"
cannot happen.

On replay: each valid CRC frame replaces that level's hash list. On first bad
CRC / short read, stop and truncate.

*/

const hashSize = 32 // sha256

const (
	levelL0 byte = 0
	levelL1 byte = 1
)

type Manifest struct {
	path      string
	handle    *os.File
	l0Hashes  []string
	l1Hashes  []string
}

func newManifest(path string) (*Manifest, error) {
	handle, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var l0, l1 []string
	offset := 0

	for offset+9 <= len(data) {
		checksum := binary.BigEndian.Uint32(data[offset : offset+4])
		level := data[offset+4]
		entriesLen := binary.BigEndian.Uint32(data[offset+5 : offset+9])
		end := offset + 9 + int(entriesLen)*hashSize
		if end > len(data) {
			break
		}
		if crc32.ChecksumIEEE(data[offset+4:end]) != checksum {
			break
		}

		if level != levelL0 && level != levelL1 {
			// unknown level byte — treat as corrupt
			break
		}

		hashes := make([]string, entriesLen)
		for i := range hashes {
			off := offset + 9 + i*hashSize
			hashes[i] = hex.EncodeToString(data[off : off+hashSize])
		}
		if level == levelL0 {
			l0 = hashes
		} else {
			l1 = hashes
		}

		offset = end
	}

	if offset < len(data) {
		if err := os.Truncate(path, int64(offset)); err != nil {
			return nil, err
		}
	}

	return &Manifest{
		path:     path,
		handle:   handle,
		l0Hashes: l0,
		l1Hashes: l1,
	}, nil
}

func (m *Manifest) L0Hashes() []string {
	return m.l0Hashes
}

func (m *Manifest) L1Hashes() []string {
	return m.l1Hashes
}

// Update appends a frame that replaces `level`'s hash list with `hashes`.
// Does not fsync — caller batches with Sync().
func (m *Manifest) Update(level byte, hashes []string) error {
	if level != levelL0 && level != levelL1 {
		return fmt.Errorf("manifest: invalid level %d", level)
	}

	data := make([]byte, 9+len(hashes)*hashSize)
	data[4] = level
	binary.BigEndian.PutUint32(data[5:9], uint32(len(hashes)))

	for i, s := range hashes {
		hash, err := hex.DecodeString(s)
		if err != nil {
			return err
		}
		copy(data[9+i*hashSize:], hash)
	}

	checksum := crc32.ChecksumIEEE(data[4:])
	binary.BigEndian.PutUint32(data[0:4], checksum)

	if _, err := m.handle.Write(data); err != nil {
		return err
	}

	switch level {
	case levelL0:
		m.l0Hashes = hashes
	case levelL1:
		m.l1Hashes = hashes
	}
	return nil
}

func (m *Manifest) Sync() error {
	return m.handle.Sync()
}

// syncDir fsyncs a directory so preceding renames (and the file content they
// reference, via FS metadata-after-data ordering) are durable.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return err
	}
	return dir.Close()
}
