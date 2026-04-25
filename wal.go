package protodb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

/*

WAL frame layout:

┌───────────┬──────────────┬─────┬───────────────┬───────┐
│ crc32 u32 │ key_len u32  │ key │ value_len u32 │ value │
└───────────┴──────────────┴─────┴───────────────┴───────┘

crc32:     checksum of (key_len + key + value_len + value)
value_len: byte length of value, or tombstone (0xFFFFFFFF) for deletes

Commit marker layout:

┌─────────────┬──────────────────┐
│ padding u32 │ key_len = 0xFFFF │  ← impossible key_len signals a commit
└─────────────┴──────────────────┘

┌───────────┐
│   frame   │
│   frame   │
│   frame   │
│   commit  │
│   frame   │
│   frame   │
│   commit  │
│   ...     │
└───────────┘

On replay, frames are buffered until a commit marker is seen.
A bad checksum or short read stops replay. Uncommitted
trailing frames are silently dropped (normal after a crash).

*/

const walChecksumSize = 4
const walTombstone uint32 = 0xFFFFFFFF
const commitKeyLen uint32 = 0xFFFFFFFF

const DefaultWALFlushBytes int = 32 * 1024 // 32Kb
const DefaultWALSyncBytes int = 0          // Rely on the OS

type WAL struct {
	fs             FS
	path           string
	handle         File
	buf            bytes.Buffer
	unsyncedBytes  int
	flushThreshold int
	syncThreshold  int // fsync threshold; 0 = never auto-sync, rely on OS
}

func newWAL(fs FS, path string) (*WAL, error) {
	return &WAL{
		fs:             fs,
		path:           path,
		handle:         nil,
		unsyncedBytes:  0,
		flushThreshold: DefaultWALFlushBytes,
		syncThreshold:  DefaultWALSyncBytes,
	}, nil
}

func (wal *WAL) open() error {
	if wal.handle != nil {
		return nil
	}
	handle, err := wal.fs.OpenAppend(wal.path)
	if err != nil {
		return err
	}
	wal.handle = handle
	return nil
}

func (wal *WAL) Append(key Key, value []byte) error {
	batch := wal.Batch()
	batch.Put(key, value)
	return batch.Commit()
}

func (wal *WAL) flush() error {
	if wal.buf.Len() == 0 {
		return nil
	}
	err := wal.open()
	if err != nil {
		return err
	}
	n, err := wal.handle.Write(wal.buf.Bytes())
	// Drop whatever bytes made it to the fd — the fd position advanced by n,
	// so on retry we must not re-write them.
	wal.buf.Next(n)
	return err
}

func (wal *WAL) sync() error {
	err := wal.flush()
	if err != nil {
		return err
	}
	if wal.handle == nil {
		return nil
	}
	err = wal.handle.Sync()
	if err != nil {
		return err
	}
	wal.unsyncedBytes = 0
	return nil
}

type WALBatch struct {
	wal      *WAL
	buf      bytes.Buffer
	byteSize int
}

func (wal *WAL) Batch() WALBatch {
	return WALBatch{wal: wal}
}

// Grow pre-allocates batch buffer capacity. Callers that know the
// approximate serialized payload size (e.g. commitLoop has tx.byteSize)
// use this to avoid ~log2(N) incremental bytes.Buffer reallocations as
// frames are appended.
func (batch *WALBatch) Grow(n int) {
	batch.buf.Grow(n)
}

func (batch *WALBatch) Put(key Key, value []byte) {
	writeFrame(&batch.buf, key, value)
	batch.byteSize += len(key) + len(value)
}

func (batch *WALBatch) Delete(key Key) {
	writeFrame(&batch.buf, key, nil)
	batch.byteSize += len(key)
}

func (wal *WAL) maybeFlush() error {
	if wal.buf.Len() >= wal.flushThreshold {
		return wal.flush()
	}
	return nil
}

func (wal *WAL) maybeSync() error {
	if wal.syncThreshold > 0 && wal.unsyncedBytes >= wal.syncThreshold {
		return wal.sync()
	}
	return nil
}

func (batch *WALBatch) Commit() error {
	wal := batch.wal
	data := batch.buf.Bytes()
	wal.buf.Write(data)
	writeU32(&wal.buf, 0)
	writeU32(&wal.buf, commitKeyLen)
	wal.unsyncedBytes += len(data) + 8

	if err := wal.maybeFlush(); err != nil {
		return err
	}
	return wal.maybeSync()
}

func writeFrame(buf *bytes.Buffer, key Key, value []byte) {
	valueLen := uint32(len(value))
	if value == nil {
		valueLen = walTombstone
	}

	crcStart := buf.Len()
	writeU32(buf, 0) // CRC placeholder, patched below
	writeU32(buf, uint32(len(key)))
	buf.Write(key)
	writeU32(buf, valueLen)
	buf.Write(value)

	data := buf.Bytes()
	binary.BigEndian.PutUint32(data[crcStart:], crc32.ChecksumIEEE(data[crcStart+walChecksumSize:]))
}

// Clear resets the WAL to empty. Closes the current handle, truncates the
// file to 0 via FS, and leaves the handle nil — the next Append will lazily
// reopen via open(). Done this way (instead of File.Truncate) so the File
// interface stays a subset of Pebble's vfs.File.
//
// If the WAL file was never opened (handle still nil), this is a no-op —
// there's nothing on disk to truncate.
func (wal *WAL) Clear() error {
	wal.buf.Reset()
	wal.unsyncedBytes = 0
	if wal.handle == nil {
		return nil
	}
	if err := wal.handle.Close(); err != nil {
		return err
	}
	wal.handle = nil
	return wal.fs.Truncate(wal.path, 0)
}

func (wal *WAL) Close() error {
	// Sync any unsynced bytes before closing — clean shutdown must be durable.
	err := wal.sync()
	if err != nil {
		return err
	}
	if wal.handle == nil {
		return nil
	}
	err = wal.handle.Close()
	wal.handle = nil
	return err
}

func (wal *WAL) Drop() error {
	wal.buf.Reset()
	wal.unsyncedBytes = 0
	if wal.handle != nil {
		_ = wal.handle.Close()
		wal.handle = nil
	}
	return wal.fs.Truncate(wal.path, 0)
}

func (wal *WAL) replay(table *memtable) (int, error) {
	data, err := wal.fs.ReadFile(wal.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	entries := make([]KeyValue, 0)
	offset := 0
	comittedOffset := 0

	replayCommit := func() {
		for _, entry := range entries {
			if entry.Value == nil {
				table.Delete(entry.Key, 0)
			} else {
				table.Put(entry.Key, entry.Value, 0)
			}
		}
		comittedOffset = offset
		entries = entries[:0]
	}

	for offset+8 <= len(data) {
		checksum := binary.BigEndian.Uint32(data[offset : offset+4])
		keyLen := binary.BigEndian.Uint32(data[offset+4 : offset+8])
		offset += 8

		if keyLen == commitKeyLen {
			replayCommit()
			continue
		}

		payloadStart := offset - 4 // keyLen is part of the payload

		if offset+int(keyLen) > len(data) {
			break
		}
		key := data[offset : offset+int(keyLen)]
		offset += int(keyLen)

		if offset+4 > len(data) {
			break
		}
		valueLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		var value []byte
		var payloadEnd int

		if valueLen == walTombstone {
			value = nil
		} else {
			if offset+int(valueLen) > len(data) {
				break
			}
			value = data[offset : offset+int(valueLen)]
			offset += int(valueLen)
		}
		payloadEnd = offset

		if crc32.ChecksumIEEE(data[payloadStart:payloadEnd]) != checksum {
			break
		}

		entries = append(entries, KeyValue{key, value})
	}

	return len(data) - comittedOffset, nil
}
