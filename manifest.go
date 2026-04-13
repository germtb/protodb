package protodb

import (
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"os"
)

type Manifest struct {
	path   string
	handle *os.File
	hashes []string
}

/*

Manifest layout:

A list of snapshots

┌───────────┬──────────┬────────────────┐
│ crc32 u32 │ len u32  │ hash [32]byte  │
└───────────┴──────────┴────────────────┘

*/

const hashSize = 32 // sha256

func newManifest(path string) (*Manifest, error) {
	handle, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	offset := 0
	var snapshot []byte

	for offset+8 <= len(data) {
		checksum := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		entriesLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(entriesLen)*hashSize > len(data) {
			break
		}

		hashes := data[offset : offset+int(entriesLen)*hashSize]

		if crc32.ChecksumIEEE(data[offset-4:offset+int(entriesLen)*hashSize]) != checksum {
			break
		}

		offset += int(entriesLen) * hashSize

		snapshot = hashes
	}

	if offset < len(data) {
		if err := os.Truncate(path, int64(offset)); err != nil {
			return nil, err
		}
	}

	hashes := make([]string, len(snapshot)/hashSize)

	for i := range len(snapshot) / hashSize {
		hashes[i] = hex.EncodeToString(snapshot[i*hashSize : (i+1)*hashSize])
	}

	return &Manifest{path: path, hashes: hashes, handle: handle}, nil
}

func (m *Manifest) Hashes() []string {
	return m.hashes[:]
}

func (m *Manifest) TrimEnd(l int) error {
	new_hashes := m.hashes[:len(m.hashes)-l]
	return m.Update(new_hashes)
}

func (m *Manifest) Update(hashes []string) error {
	data := make([]byte, len(hashes)*hashSize+8)

	binary.BigEndian.PutUint32(data[4:], uint32(len(hashes)))

	for i, s := range hashes {
		hash, err := hex.DecodeString(s)
		if err != nil {
			return err
		}
		copy(data[8+i*hashSize:], hash)
	}

	checksum := crc32.ChecksumIEEE(data[4:])
	binary.BigEndian.PutUint32(data[0:], checksum)

	_, err := m.handle.Write(data)
	if err != nil {
		return err
	}
	m.hashes = hashes
	return nil
}

func (m *Manifest) Sync() error {
	return m.handle.Sync()
}
