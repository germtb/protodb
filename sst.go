package protodb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrUnsortedKeys       = errors.New("protodb: keys must be in strictly ascending order")
	ErrUnsupportedVersion = errors.New("protodb: unsupported sst version")
	ErrTailTooSmall       = errors.New("protodb: tail byte size smaller than footer")
	ErrNotFound           = errors.New("protodb: key not found")
	ErrDeleted            = errors.New("protodb: key deleted")
	ErrCorrupted          = errors.New("protodb: corrupted sst")
)

/*

This struct holds the metadata part of an sst, that is, everything
except the values. This makes it very small in memory.
The layout for an sst  is:

┌─────────────────────┐
│   blocks            │  each is padded to 4Kb, but can be larger
├─────────────────────┤
│   metadata          │  extensible metadata section. It does not need a known offset because it is always at sstByteSize - footerSize - keysSize
├─────────────────────┤
│   blockIndex        │  each is a tuple like (firstKey(u64), blockOffset(u64), blockLength(u32))
├─────────────────────┤
│   blockCount        │  number of blocks (u64)
│   version           │  (u16)
└─────────────────────┘

This is the block layout

┌─────────────────────────────────┐
│   key(u64) | len(u32) | value   │
│   key(u64) | len(u32) | value   │
│   key(u64) | len(u32) | value   │
│   padded to 4KB - footerSize    │
├─────────────────────────────────┤
│   entryCount(u16) | crc32(u32)  │
└─────────────────────────────────┘

*/

type sstBlockIndex struct {
	FirstKey uint64
	Offset   uint64
	Length   uint32
}

type sstFooter struct {
	BlockCount uint64
	Version    uint16
}

type sstBlock struct {
	data []byte
}

type sst struct {
	index      uint64
	cacheMutex sync.Mutex
	cache      *LRU[uint64, sstBlock]
	blocks     []sstBlockIndex
	footer     sstFooter
	hash       string
	path       string
	fileSize   int64
}

type reader interface {
	ReadAt(buffer []byte, offset int64) (int, error)
}

const sstVersion uint16 = 1
const tombstone uint32 = 0xFFFFFFFF

var footerSize int64 = int64(binary.Size(sstFooter{}))
var sstBlockIndexSize int64 = int64(binary.Size(sstBlockIndex{}))
var keySize int64 = 8 // uint64
var lenSize int64 = 4 // uint32
var keyLenSize int64 = keySize + lenSize
var blockFooterSize int64 = 6 // uint16 + uint64

type ReaderOptions struct {
	TailByteSize int64
}

var BlockSize int = 1024 * 4       // 4Kb
var SSTSize int = 1024 * 1024 * 16 // 16 Mb

func WriteSST(path string, entries Iterator) ([]*sst, error) {
	tempfile, err := os.CreateTemp(path, "-temp-")

	if err != nil {
		return nil, err
	}

	defer func() {
		tempfile.Close()
		os.Remove(tempfile.Name())
	}()

	var block bytes.Buffer
	block.Grow(BlockSize)
	var blocks = make([]sstBlockIndex, 0)
	var ssts = make([]*sst, 0)
	var blockEntries uint16 = 0
	var lastKey uint64 = 0
	var firstKey uint64 = 0
	var offset uint64 = 0
	var firstIteration bool = true

	finishBlock := func() error {
		binary.Write(&block, binary.BigEndian, blockEntries)
		checksum := crc32.ChecksumIEEE(block.Bytes())
		binary.Write(&block, binary.BigEndian, checksum)

		blocks = append(blocks, sstBlockIndex{
			FirstKey: firstKey,
			Offset:   offset,
			Length:   uint32(block.Len()),
		})

		_, err := tempfile.Write(block.Bytes())

		if err != nil {
			return err
		}

		offset += uint64(block.Len())
		blockEntries = 0

		block = bytes.Buffer{}
		block.Grow(BlockSize)

		return nil
	}

	finishSST := func() error {
		if blockEntries > 0 {
			err = finishBlock()

			if err != nil {
				return err
			}
		}

		for _, blockIndex := range blocks {
			binary.Write(tempfile, binary.BigEndian, blockIndex)
		}

		footer := sstFooter{
			BlockCount: uint64(len(blocks)),
			Version:    sstVersion,
		}

		binary.Write(tempfile, binary.BigEndian, footer)

		if err = tempfile.Sync(); err != nil {
			return err
		}

		tempfile.Seek(0, 0)
		hasher := sha256.New()
		io.Copy(hasher, tempfile)
		hash := hex.EncodeToString(hasher.Sum(nil))

		finalPath := filepath.Join(path, hash)
		err = os.Rename(tempfile.Name(), finalPath)

		if err != nil {
			return err
		}

		ssts = append(ssts, &sst{
			cache:    newLRU[uint64, sstBlock](128, nil),
			blocks:   blocks,
			footer:   footer,
			hash:     hash,
			path:     finalPath,
			fileSize: int64(offset) + int64(len(blocks))*sstBlockIndexSize + footerSize,
		})

		offset = 0
		firstKey = 0
		firstIteration = true
		blocks = make([]sstBlockIndex, 0)
		tempfile, err = os.CreateTemp(path, "-temp-")

		return err
	}

	// First write all the blocks. Whenever a block exceeds 4Kb we finish it and
	// start a new one.
	for entries.Next() {
		key := entries.Key()
		value := entries.Value()

		// This is to ensure entries are in sorted order
		if !firstIteration && key <= lastKey {
			return nil, fmt.Errorf("%w: %d <= %d", ErrUnsortedKeys, key, lastKey)
		} else {
			firstIteration = false
			lastKey = key
		}

		if block.Len() > BlockSize {
			err = finishBlock()

			if err != nil {
				return nil, err
			}
		}

		fileSize := int64(offset) + int64(len(blocks))*sstBlockIndexSize + footerSize

		if fileSize > int64(SSTSize) {
			err = finishSST()

			if err != nil {
				return nil, err
			}
		}

		if blockEntries == 0 {
			firstKey = key
		}

		if value == nil {
			binary.Write(&block, binary.BigEndian, key)
			binary.Write(&block, binary.BigEndian, tombstone)
		} else {
			binary.Write(&block, binary.BigEndian, key)
			binary.Write(&block, binary.BigEndian, uint32(len(value)))
			block.Write(value)
		}

		blockEntries += 1
	}

	err = finishSST()

	if err != nil {
		return nil, err
	}

	return ssts, nil
}

func ReadSST(path string, hash string, options *ReaderOptions) (*sst, error) {
	path = filepath.Join(path, hash)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	// We optimisitcally read the last 64Kb, because most ssts will not have that many entries
	var tailByteSize int64 = 64 * 1024

	if options != nil {
		tailByteSize = options.TailByteSize
	}

	if tailByteSize < footerSize {
		return nil, ErrTailTooSmall
	}

	if tailByteSize > fileSize {
		tailByteSize = fileSize
	}
	if tailByteSize < footerSize {
		return nil, fmt.Errorf("%w: file too small (%d bytes)", ErrCorrupted, fileSize)
	}
	tail := make([]byte, tailByteSize)
	if _, err := file.ReadAt(tail, fileSize-tailByteSize); err != nil {
		return nil, err
	}

	var footer sstFooter
	if err = binary.Read(bytes.NewReader(tail[int64(len(tail))-footerSize:]), binary.BigEndian, &footer); err != nil {
		return nil, err
	}

	if footer.Version != sstVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, footer.Version, sstVersion)
	}

	// We optimistically loaded a tail of a certain size, so we need to check if the block index is larger than that.
	// Guard against overflow and corruption: the block index + footer must fit within the file.
	maxEntries := uint64(fileSize-footerSize) / uint64(sstBlockIndexSize)
	if footer.BlockCount > maxEntries {
		return nil, fmt.Errorf("%w: footer claims %d entries but file can hold at most %d", ErrCorrupted, footer.BlockCount, maxEntries)
	}

	var neededBytes int64 = footerSize + int64(footer.BlockCount)*sstBlockIndexSize

	if neededBytes > fileSize {
		return nil, fmt.Errorf("%w: footer claims %d entries but file is only %d bytes", ErrCorrupted, footer.BlockCount, fileSize)
	}

	if neededBytes > tailByteSize {
		tail = make([]byte, neededBytes)
		tailByteSize = neededBytes
		if _, err := file.ReadAt(tail, fileSize-neededBytes); err != nil {
			return nil, err
		}
	}

	blocksStart := int64(len(tail)) - footerSize - int64(footer.BlockCount)*sstBlockIndexSize
	blocksEnd := int64(len(tail)) - footerSize
	var blocks = make([]sstBlockIndex, footer.BlockCount)

	if err = binary.Read(bytes.NewReader(tail[blocksStart:blocksEnd]), binary.BigEndian, &blocks); err != nil {
		return nil, err
	}

	return &sst{
		cache:    newLRU[uint64, sstBlock](128, nil),
		blocks:   blocks,
		footer:   footer,
		hash:     hash,
		path:     path,
		fileSize: fileSize,
	}, nil
}

func (s *sst) FirstKey() uint64 {
	if len(s.blocks) == 0 {
		return 0
	}

	return s.blocks[0].FirstKey
}

func (s *sst) valuesEnd() int64 {
	return s.fileSize - footerSize - int64(s.footer.BlockCount)*int64(sstBlockIndexSize)
}

func (s *sst) GetBlock(blockIndex uint64, reader reader) (*sstBlock, error) {
	s.cacheMutex.Lock()
	cached, ok := s.cache.Get(blockIndex)
	s.cacheMutex.Unlock()

	if ok {
		return &cached, nil
	}

	block := s.blocks[blockIndex]
	data := make([]byte, block.Length)
	_, err := reader.ReadAt(data, int64(block.Offset))

	if err != nil {
		return nil, err
	}

	if len(data) < 6 {
		return nil, ErrCorrupted
	}
	storedChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != storedChecksum {
		return nil, ErrCorrupted
	}

	result := sstBlock{data}
	s.cacheMutex.Lock()
	s.cache.Put(blockIndex, result)
	s.cacheMutex.Unlock()
	return &result, nil
}

func (s *sst) Get(key uint64, reader reader) ([]byte, error) {
	blockIndex := sort.Search(len(s.blocks), func(i int) bool {
		return s.blocks[i].FirstKey > key
	}) - 1

	if blockIndex < 0 {
		return nil, ErrNotFound
	}

	block, err := s.GetBlock(uint64(blockIndex), reader)
	if err != nil {
		return nil, err
	}

	// Linear scan within the block to find the key
	// Block layout: [key(u64) | len(u32) | value(len)] ... [count(u16) | crc32(u32)]
	data := block.data
	pos := int64(0)
	end := int64(len(data)) - blockFooterSize

	for pos <= end-keyLenSize {
		entryKey := binary.BigEndian.Uint64(data[pos : pos+keySize])
		entryLen := binary.BigEndian.Uint32(data[pos+keySize : pos+keyLenSize])

		if entryKey == key {
			if entryLen == tombstone {
				return nil, ErrDeleted
			}
			return data[pos+keyLenSize : pos+keyLenSize+int64(entryLen)], nil
		}

		if entryLen == tombstone {
			pos += keyLenSize
		} else {
			pos += keyLenSize + int64(entryLen)
		}
	}

	return nil, ErrNotFound
}

type sstIterator struct {
	sst        *sst
	reader     reader
	lo         uint64
	hi         uint64
	blockIndex int
	block      *sstBlock
	pos        int64
	end        int64
	key        uint64
	value      []byte
	done       bool
}

func (s *sst) Iterator(lo uint64, hi uint64, r reader) *sstIterator {
	blockIndex := sort.Search(len(s.blocks), func(i int) bool {
		return s.blocks[i].FirstKey > lo
	}) - 1

	if blockIndex < 0 {
		blockIndex = 0
	}

	return &sstIterator{
		sst:        s,
		reader:     r,
		lo:         lo,
		hi:         hi,
		blockIndex: blockIndex,
		done:       blockIndex >= len(s.blocks),
	}
}

func (it *sstIterator) Next() bool {
	if it.done {
		return false
	}

	for {
		// Load block if needed
		if it.block == nil {
			if it.blockIndex >= len(it.sst.blocks) {
				it.done = true
				return false
			}
			block, err := it.sst.GetBlock(uint64(it.blockIndex), it.reader)
			if err != nil {
				it.done = true
				return false
			}
			it.block = block
			it.pos = 0
			it.end = int64(len(block.data)) - blockFooterSize
		}

		// Advance to next block if current is exhausted
		if it.pos > it.end-keyLenSize {
			it.blockIndex++
			it.block = nil
			continue
		}

		data := it.block.data
		entryKey := binary.BigEndian.Uint64(data[it.pos : it.pos+keySize])
		entryLen := binary.BigEndian.Uint32(data[it.pos+keySize : it.pos+keyLenSize])

		// Advance position
		if entryLen == tombstone {
			it.pos += keyLenSize
		} else {
			it.pos += keyLenSize + int64(entryLen)
		}

		if entryKey >= it.hi {
			it.done = true
			return false
		}

		if entryKey >= it.lo {
			it.key = entryKey
			if entryLen == tombstone {
				it.value = nil
			} else {
				it.value = data[it.pos-int64(entryLen) : it.pos]
			}
			return true
		}
	}
}

func (it *sstIterator) Key() uint64 {
	return it.key
}

func (it *sstIterator) Value() []byte {
	return it.value
}
