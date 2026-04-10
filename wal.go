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
// ┌───────────┬──────────────┬─────┬──────────────┬───────┐
// │ crc32 u32 │ key_len u32  │ key │ value_len u32│ value │
// └───────────┴──────────────┴─────┴──────────────┴───────┘
//
// crc32:     checksum of (key_len + key + value_len + value)
// value_len: byte length of value, or tombstone (0xFFFFFFFF) for deletes
//
// On replay, a frame with a bad checksum or short read stops replay.
// The file is truncated to the end of the last valid frame.

const walChecksumSize = 4
const keyLenSize = 4
const valueLenSize = 4
const walTombstone uint32 = 0xFFFFFFFF

type WAL struct {
	path       string
	handle     *os.File
	buf        bytes.Buffer
	bufferSize int
}

func newWAL(path string, bufferSize int) (*WAL, error) {
	return &WAL{
		path:       path,
		handle:     nil,
		bufferSize: bufferSize,
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

func (wal *WAL) flush() error {
	if wal.buf.Len() == 0 {
		return nil
	}
	err := wal.open()
	if err != nil {
		return err
	}
	_, err = wal.handle.Write(wal.buf.Bytes())
	wal.buf.Reset()
	return err
}

type WALBatch struct {
	wal      *WAL
	buf      bytes.Buffer
	byteSize int
}

func (wal *WAL) Batch() WALBatch {
	return WALBatch{wal: wal}
}

func (batch *WALBatch) Put(key Key, value []byte) {
	writeFrame(&batch.buf, key, value)
	batch.byteSize += len(key) + len(value)
}

func (batch *WALBatch) Delete(key Key) {
	writeFrame(&batch.buf, key, nil)
	batch.byteSize += len(key)
}

func (batch *WALBatch) Commit() error {
	return batch.wal.Write(batch.buf.Bytes())
}

func (wal *WAL) Append(key Key, value []byte) error {
	batch := wal.Batch()
	batch.Put(key, value)
	return batch.Commit()
}

func writeFrame(buf *bytes.Buffer, key Key, value []byte) {
	var valueLen uint32
	if value == nil {
		valueLen = walTombstone
	} else {
		valueLen = uint32(len(value))
	}

	frameSize := walChecksumSize + keyLenSize + len(key) + valueLenSize + len(value)
	frame := make([]byte, frameSize)

	// Build payload: key_len | key | value_len | value
	pos := walChecksumSize
	binary.BigEndian.PutUint32(frame[pos:], uint32(len(key)))
	pos += 4
	copy(frame[pos:], key)
	pos += len(key)
	binary.BigEndian.PutUint32(frame[pos:], valueLen)
	pos += 4
	if value != nil {
		copy(frame[pos:], value)
	}

	// Compute checksum over payload and write it at the start
	binary.BigEndian.PutUint32(frame[0:4], crc32.ChecksumIEEE(frame[walChecksumSize:]))

	buf.Write(frame)
}

func (wal *WAL) Write(data []byte) error {
	wal.buf.Write(data)
	if wal.buf.Len() >= wal.bufferSize {
		return wal.flush()
	}
	return nil
}

func (wal *WAL) Sync() error {
	err := wal.flush()
	if err != nil {
		return err
	}
	if wal.handle == nil {
		return nil
	}
	return wal.handle.Sync()
}

func (wal *WAL) Clear() error {
	wal.buf.Reset()
	if wal.handle == nil {
		return nil
	}
	err := wal.handle.Truncate(0)
	if err != nil {
		return err
	}
	_, err = wal.handle.Seek(0, 0)
	return err
}

func (wal *WAL) Close() error {
	err := wal.flush()
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
		// Read checksum
		var checksumBuf [4]byte
		if _, err := io.ReadFull(reader, checksumBuf[:]); err != nil {
			break // EOF
		}
		checksum := binary.BigEndian.Uint32(checksumBuf[:])

		// Mark start of payload for checksum verification
		payloadStart := int64(len(data)) - int64(reader.Len())

		// Read key_len
		var scratch [4]byte
		if _, err := io.ReadFull(reader, scratch[:]); err != nil {
			break // truncated
		}
		keyLen := binary.BigEndian.Uint32(scratch[:])

		// Read key
		if int64(reader.Len()) < int64(keyLen)+4 {
			break // truncated
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			break
		}

		// Read value_len
		if _, err := io.ReadFull(reader, scratch[:]); err != nil {
			break // truncated
		}
		valueLen := binary.BigEndian.Uint32(scratch[:])

		// Read value
		var value []byte
		if valueLen == walTombstone {
			// tombstone, no value bytes
		} else {
			if int64(reader.Len()) < int64(valueLen) {
				break // truncated
			}
			value = make([]byte, valueLen)
			if _, err := io.ReadFull(reader, value); err != nil {
				break
			}
		}

		// Verify checksum over the payload
		payloadEnd := int64(len(data)) - int64(reader.Len())
		payload := data[payloadStart:payloadEnd]
		if crc32.ChecksumIEEE(payload) != checksum {
			break // corrupt frame
		}

		if valueLen == walTombstone {
			table.Delete(key)
		} else {
			table.Put(key, value)
		}

		lastGoodOffset = payloadEnd
	}

	// Truncate to last good frame to clean up any partial write
	if lastGoodOffset < int64(len(data)) {
		os.Truncate(wal.path, lastGoodOffset)
	}

	return nil
}
