package protodb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// WAL frame layout:
//
// ┌───────────────┬───────────┬──────────┬──────────┬───────────┐
// │ frame_len u32 │ crc32 u32 │ key u64  │ len u64  │ value     │
// └───────────────┴───────────┴──────────┴──────────┴───────────┘
//
// frame_len: size of everything after frame_len (crc32 + key + len + value)
// crc32:     checksum of (key + len + value)
// len:       byte length of value, or tombstone (^uint64(0)) for deletes
//
// On replay, a frame with a bad checksum or short read stops replay.
// The file is truncated to the end of the last valid frame.

const walHeaderSize = 4 + 4     // frame_len + crc32
const walEntryFixedSize = 8 + 8 // key + len
const walTombstone uint64 = ^uint64(0)

type WAL struct {
	path   string
	handle *os.File
}

func newWAL(path string) (*WAL, error) {
	return &WAL{
		path:   path,
		handle: nil,
	}, nil
}

func (wal *WAL) open() error {
	if wal.handle != nil {
		return nil
	}
	handle, err := os.OpenFile(wal.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	wal.handle = handle
	return nil
}

func (wal *WAL) Append(key uint64, value []byte) error {
	if err := wal.open(); err != nil {
		return err
	}

	var buf bytes.Buffer
	writeFrame(&buf, key, value)
	return wal.Write(buf.Bytes())
}

func writeFrame(buf *bytes.Buffer, key uint64, value []byte) {
	var valueLen uint64
	if value == nil {
		valueLen = walTombstone
	} else {
		valueLen = uint64(len(value))
	}

	payloadSize := walEntryFixedSize + len(value)

	// Reserve space for header, write payload directly into buffer
	headerStart := buf.Len()
	buf.Grow(walHeaderSize + payloadSize)
	buf.Write(make([]byte, walHeaderSize)) // placeholder for header

	// Write payload directly into the buffer
	var fixed [walEntryFixedSize]byte
	binary.BigEndian.PutUint64(fixed[0:8], key)
	binary.BigEndian.PutUint64(fixed[8:16], valueLen)
	buf.Write(fixed[:])
	if value != nil {
		buf.Write(value)
	}

	// Compute checksum over the payload we just wrote
	payload := buf.Bytes()[headerStart+walHeaderSize:]
	frame := buf.Bytes()[headerStart:]
	binary.BigEndian.PutUint32(frame[0:4], uint32(4+payloadSize))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
}

func (wal *WAL) Write(data []byte) error {
	if err := wal.open(); err != nil {
		return err
	}
	_, err := wal.handle.Write(data)
	return err
}

func (wal *WAL) Sync() error {
	if wal.handle == nil {
		return nil
	}
	return wal.handle.Sync()
}

func (wal *WAL) Clear() error {
	if wal.handle == nil {
		return nil
	}
	if err := wal.handle.Truncate(0); err != nil {
		return err
	}
	_, err := wal.handle.Seek(0, 0)
	return err
}

func (wal *WAL) Close() error {
	if wal.handle == nil {
		return nil
	}
	err := wal.handle.Close()
	wal.handle = nil
	return err
}

func (wal *WAL) Drop() error {
	if wal.handle == nil {
		return nil
	}
	wal.handle.Truncate(0)
	return wal.handle.Close()
}

func (wal *WAL) replay(table *memtable) error {
	data, err := os.ReadFile(wal.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	reader := bytes.NewReader(data)
	var lastGoodOffset int64

	for {
		// Read frame header
		var frameLen uint32
		var checksum uint32
		if err := binary.Read(reader, binary.BigEndian, &frameLen); err != nil {
			break // EOF
		}
		if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
			break // truncated header
		}

		// Read payload (frameLen includes the 4-byte checksum we already read)
		payloadSize := int(frameLen) - 4
		if payloadSize < walEntryFixedSize {
			break // corrupt frame length
		}

		payload := make([]byte, payloadSize)
		if _, err := io.ReadFull(reader, payload); err != nil {
			break // truncated payload
		}

		// Verify checksum
		if crc32.ChecksumIEEE(payload) != checksum {
			break // corrupt frame
		}

		// Parse entry
		key := binary.BigEndian.Uint64(payload[0:8])
		valueLen := binary.BigEndian.Uint64(payload[8:16])

		if valueLen == walTombstone {
			table.Delete(key)
		} else {
			value := payload[16:]
			table.Put(key, value)
		}

		lastGoodOffset = int64(len(data)) - int64(reader.Len())
	}

	// Truncate to last good frame to clean up any partial write
	if lastGoodOffset < int64(len(data)) {
		os.Truncate(wal.path, lastGoodOffset)
	}

	return nil
}
