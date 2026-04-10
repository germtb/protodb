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

SST layout:

┌─────────────────────┐
│   blocks            │  each is ~4Kb, but can be larger
├─────────────────────┤
│   blockIndex        │  variable-length entries: key_len(u32) | key | offset(u64) | length(u32)
├─────────────────────┤
│   blockIndexSize    │  byte size of blockIndex section (u64)
│   blockCount        │  number of blocks (u64)
│   version           │  (u16)
└─────────────────────┘

Block layout:

┌──────────────────────────────────────────────────┐
│   key_len(u32) | key | value_len(u32) | value    │
│   key_len(u32) | key | value_len(u32) | value    │
│   ...                                            │
├──────────────────────────────────────────────────┤
│   entryCount(u16) | crc32(u32)                   │
└──────────────────────────────────────────────────┘

*/

type sstBlockIndex struct {
	FirstKey Key
	Offset   uint64
	Length   uint32
}

type sstFooter struct {
	BlockIndexSize uint64
	BlockCount     uint64
	Version        uint16
}

type sstBlock struct {
	data []byte
}

type sst struct {
	cacheMutex sync.Mutex
	cache      *LRU[uint64, sstBlock]
	blocks     []sstBlockIndex
	footer     sstFooter
	hash       string
	path       string
	fileSize   int64
	firstKey   Key
}

type reader interface {
	ReadAt(buffer []byte, offset int64) (int, error)
}

const sstVersion uint16 = 1
const tombstone uint32 = 0xFFFFFFFF

var footerSize int64 = 8 + 8 + 2 // BlockIndexSize + BlockCount + Version
var blockFooterSize int64 = 6    // entryCount(u16) + crc32(u32)

type ReaderOptions struct {
	TailByteSize int64
}

var BlockSize int = 1024 * 4       // 4Kb
var SSTSize int = 1024 * 1024 * 16 // 16 Mb

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func writeBlockIndex(writer io.Writer, index sstBlockIndex) {
	var buf [16]byte // key_len(4) + offset(8) + length(4)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(index.FirstKey)))
	writer.Write(buf[0:4])
	writer.Write(index.FirstKey)
	binary.BigEndian.PutUint64(buf[0:8], index.Offset)
	binary.BigEndian.PutUint32(buf[8:12], index.Length)
	writer.Write(buf[0:12])
}

func readBlockIndex(reader *bytes.Reader) (sstBlockIndex, error) {
	var buf [12]byte // key_len(4) + offset(8) or offset(8) + length(4)

	if _, err := io.ReadFull(reader, buf[0:4]); err != nil {
		return sstBlockIndex{}, err
	}
	keyLen := binary.BigEndian.Uint32(buf[0:4])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return sstBlockIndex{}, err
	}

	if _, err := io.ReadFull(reader, buf[0:12]); err != nil {
		return sstBlockIndex{}, err
	}
	offset := binary.BigEndian.Uint64(buf[0:8])
	length := binary.BigEndian.Uint32(buf[8:12])

	return sstBlockIndex{FirstKey: key, Offset: offset, Length: length}, nil
}

func WriteSST(path string, entries Iterator) ([]*sst, error) {
	var ssts []*sst
	var buf bytes.Buffer
	buf.Grow(BlockSize)
	var block bytes.Buffer
	block.Grow(BlockSize)
	var blocks []sstBlockIndex
	var blockEntries uint16 = 0
	var lastKey Key
	var firstKey Key
	var offset uint64 = 0
	var firstIteration bool = true

	finishBlock := func() {
		var footer [6]byte // entryCount(2) + crc32(4)
		binary.BigEndian.PutUint16(footer[0:2], blockEntries)
		block.Write(footer[0:2])
		checksum := crc32.ChecksumIEEE(block.Bytes())
		binary.BigEndian.PutUint32(footer[2:6], checksum)
		block.Write(footer[2:6])

		blocks = append(blocks, sstBlockIndex{
			FirstKey: firstKey,
			Offset:   offset,
			Length:   uint32(block.Len()),
		})

		buf.Write(block.Bytes())
		offset += uint64(block.Len())
		blockEntries = 0

		block = bytes.Buffer{}
		block.Grow(BlockSize)
	}

	finishSST := func() error {
		if blockEntries > 0 {
			finishBlock()
		}

		// Write variable-length block index
		blockIndexStart := buf.Len()
		for _, blockIndex := range blocks {
			writeBlockIndex(&buf, blockIndex)
		}
		blockIndexSize := uint64(buf.Len() - blockIndexStart)

		footer := sstFooter{
			BlockIndexSize: blockIndexSize,
			BlockCount:     uint64(len(blocks)),
			Version:        sstVersion,
		}

		var footerBuf [18]byte // BlockIndexSize(8) + BlockCount(8) + Version(2)
		binary.BigEndian.PutUint64(footerBuf[0:8], footer.BlockIndexSize)
		binary.BigEndian.PutUint64(footerBuf[8:16], footer.BlockCount)
		binary.BigEndian.PutUint16(footerBuf[16:18], footer.Version)
		buf.Write(footerBuf[:])

		// Hash the complete SST content
		hash := hex.EncodeToString(sha256Sum(buf.Bytes()))

		// Write to temp file, sync, rename
		tempfile, err := os.CreateTemp(path, "-temp-")
		if err != nil {
			return err
		}
		defer func() {
			tempfile.Close()
			os.Remove(tempfile.Name())
		}()

		_, err = tempfile.Write(buf.Bytes())
		if err != nil {
			return err
		}
		err = tempfile.Sync()
		if err != nil {
			return err
		}

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
			fileSize: int64(buf.Len()),
			firstKey: blocks[0].FirstKey,
		})

		// Reset for next SST
		buf.Reset()
		offset = 0
		firstKey = nil
		firstIteration = true
		blocks = nil

		return nil
	}

	for entries.Next() {
		key := entries.Key()
		value := entries.Value()

		// This is to ensure entries are in sorted order
		if !firstIteration && bytes.Compare(key, lastKey) <= 0 {
			return nil, fmt.Errorf("%w: %v <= %v", ErrUnsortedKeys, key, lastKey)
		} else {
			firstIteration = false
			lastKey = key
		}

		if block.Len() > BlockSize {
			finishBlock()
		}

		// Estimate file size for SST partitioning
		fileSize := int64(offset) + footerSize

		if fileSize > int64(SSTSize) {
			if err := finishSST(); err != nil {
				return nil, err
			}
		}

		if blockEntries == 0 {
			firstKey = append(Key(nil), key...) // copy the key
		}

		// Write entry: key_len(u32) | key | value_len(u32) | value
		var scratch [4]byte
		binary.BigEndian.PutUint32(scratch[:], uint32(len(key)))
		block.Write(scratch[:])
		block.Write(key)
		if value == nil {
			binary.BigEndian.PutUint32(scratch[:], tombstone)
		} else {
			binary.BigEndian.PutUint32(scratch[:], uint32(len(value)))
		}
		block.Write(scratch[:])
		if value != nil {
			block.Write(value)
		}

		blockEntries += 1
	}

	if blockEntries > 0 || len(blocks) > 0 {
		if err := finishSST(); err != nil {
			return nil, err
		}
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

	// Read the footer first
	if fileSize < footerSize {
		return nil, fmt.Errorf("%w: file too small (%d bytes)", ErrCorrupted, fileSize)
	}

	footerBuf := make([]byte, footerSize)
	if _, err := file.ReadAt(footerBuf, fileSize-footerSize); err != nil {
		return nil, err
	}

	var footer sstFooter
	footer.BlockIndexSize = binary.BigEndian.Uint64(footerBuf[0:8])
	footer.BlockCount = binary.BigEndian.Uint64(footerBuf[8:16])
	footer.Version = binary.BigEndian.Uint16(footerBuf[16:18])

	if footer.Version != sstVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, footer.Version, sstVersion)
	}

	// Read the block index
	blockIndexEnd := fileSize - footerSize
	blockIndexStart := blockIndexEnd - int64(footer.BlockIndexSize)

	if blockIndexStart < 0 {
		return nil, fmt.Errorf("%w: block index size %d exceeds file size %d", ErrCorrupted, footer.BlockIndexSize, fileSize)
	}

	blockIndexBuf := make([]byte, footer.BlockIndexSize)
	if _, err := file.ReadAt(blockIndexBuf, blockIndexStart); err != nil {
		return nil, err
	}

	blockIndexReader := bytes.NewReader(blockIndexBuf)
	blocks := make([]sstBlockIndex, 0, footer.BlockCount)
	for range footer.BlockCount {
		blockIndex, err := readBlockIndex(blockIndexReader)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to read block index", ErrCorrupted)
		}
		blocks = append(blocks, blockIndex)
	}

	var firstKey Key
	if len(blocks) > 0 {
		firstKey = blocks[0].FirstKey
	}

	return &sst{
		cache:    newLRU[uint64, sstBlock](128, nil),
		blocks:   blocks,
		footer:   footer,
		hash:     hash,
		path:     path,
		fileSize: fileSize,
		firstKey: firstKey,
	}, nil
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

// readEntry reads a single entry from block data at the given position.
// Returns key, value_len, total bytes consumed. value_len == tombstone means deleted.
func readEntry(data []byte, pos int64) (key Key, valueLen uint32, entrySize int64, err error) {
	if pos+4 > int64(len(data)) {
		return nil, 0, 0, ErrCorrupted
	}
	keyLen := binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	if pos+int64(keyLen)+4 > int64(len(data)) {
		return nil, 0, 0, ErrCorrupted
	}
	key = data[pos : pos+int64(keyLen)]
	pos += int64(keyLen)

	valueLen = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	entrySize = 4 + int64(keyLen) + 4
	if valueLen != tombstone {
		entrySize += int64(valueLen)
	}

	return key, valueLen, entrySize, nil
}

func (s *sst) Get(key Key, reader reader) ([]byte, error) {
	// Binary search on block index to find the right block
	blockIdx := bsearchBlock(s.blocks, key) - 1

	if blockIdx < 0 {
		return nil, ErrNotFound
	}

	block, err := s.GetBlock(uint64(blockIdx), reader)
	if err != nil {
		return nil, err
	}

	// Linear scan within the block
	data := block.data
	pos := int64(0)
	end := int64(len(data)) - blockFooterSize

	for pos < end {
		entryKey, valueLen, entrySize, err := readEntry(data, pos)
		if err != nil {
			return nil, err
		}

		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
			if valueLen == tombstone {
				return nil, ErrDeleted
			}
			valueStart := pos + entrySize - int64(valueLen)
			return data[valueStart : valueStart+int64(valueLen)], nil
		}

		pos += entrySize
	}

	return nil, ErrNotFound
}

// BulkGet looks up multiple keys in this SST in a single pass.
//
// `sortedKeys` must be sorted in ascending order.
// Returns parallel slices: values[i] and errs[i] correspond to sortedKeys[i].
//   - errs[i] == nil:           found a value, values[i] is the data
//   - errs[i] == ErrDeleted:    found a tombstone, values[i] is nil
//   - errs[i] == ErrNotFound:   key is not in this SST, values[i] is nil
//
// The optimization: keys are bucketed per block. Each block is read at most once,
// and a single forward scan through the block finds all requested keys in that block.
func (s *sst) BulkGet(sortedKeys []Key, reader reader) ([][]byte, []error, error) {
	values := make([][]byte, len(sortedKeys))
	errs := make([]error, len(sortedKeys))
	for i := range errs {
		errs[i] = ErrNotFound
	}

	// Walk keys and group them by block. Both keys and blocks are sorted.
	keyIndex := 0
	blockStart := 0 // narrows the bsearch range as we go

	for keyIndex < len(sortedKeys) {
		key := sortedKeys[keyIndex]

		// Find the block for this key, narrowing search to [blockStart:]
		searchBlocks := s.blocks[blockStart:]
		offset := bsearchBlock(searchBlocks, key) - 1
		blockIndex := blockStart + offset

		if blockIndex < 0 {
			// Key is before the first block — not in this SST
			keyIndex++
			continue
		}

		// Determine the range of keys that fall within this block.
		var nextFirstKey Key
		if blockIndex+1 < len(s.blocks) {
			nextFirstKey = s.blocks[blockIndex+1].FirstKey
		}

		batchEnd := keyIndex + 1
		for batchEnd < len(sortedKeys) {
			if nextFirstKey != nil && bytes.Compare(sortedKeys[batchEnd], nextFirstKey) >= 0 {
				break
			}
			batchEnd++
		}

		// Load the block once and scan it for all keys in [keyIndex, batchEnd)
		block, err := s.GetBlock(uint64(blockIndex), reader)
		if err != nil {
			return nil, nil, err
		}

		// Single forward scan through the block, picking up all keys in [keyIndex, batchEnd)
		data := block.data
		pos := int64(0)
		endPos := int64(len(data)) - blockFooterSize
		wantedIdx := keyIndex

		for pos < endPos && wantedIdx < batchEnd {
			entryKey, valueLen, entrySize, err := readEntry(data, pos)
			if err != nil {
				return nil, nil, err
			}

			// Advance through wanted keys smaller than the current entry
			for wantedIdx < batchEnd && bytes.Compare(sortedKeys[wantedIdx], entryKey) < 0 {
				wantedIdx++
			}

			if wantedIdx >= batchEnd {
				break
			}

			if bytes.Equal(sortedKeys[wantedIdx], entryKey) {
				if valueLen == tombstone {
					errs[wantedIdx] = ErrDeleted
				} else {
					valueStart := pos + entrySize - int64(valueLen)
					values[wantedIdx] = data[valueStart : valueStart+int64(valueLen)]
					errs[wantedIdx] = nil
				}
				wantedIdx++
			}

			pos += entrySize
		}

		keyIndex = batchEnd
		blockStart = blockIndex
	}

	return values, errs, nil
}

type sstIterator struct {
	sst        *sst
	reader     reader
	lo         Key
	hi         Key
	blockIndex int
	block      *sstBlock
	pos        int64
	end        int64
	key        Key
	value      []byte
	done       bool
}

func (s *sst) Iterator(lo Key, hi Key, r reader) *sstIterator {
	blockIndex := bsearchBlock(s.blocks, lo) - 1

	blockIndex = max(blockIndex, 0)

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
		if it.pos >= it.end {
			it.blockIndex++
			it.block = nil
			continue
		}

		data := it.block.data
		entryKey, valueLen, entrySize, err := readEntry(data, it.pos)
		if err != nil {
			it.done = true
			return false
		}

		it.pos += entrySize

		if it.hi != nil && bytes.Compare(entryKey, it.hi) >= 0 {
			it.done = true
			return false
		}

		if it.lo == nil || bytes.Compare(entryKey, it.lo) >= 0 {
			it.key = entryKey
			if valueLen == tombstone {
				it.value = nil
			} else {
				valueStart := it.pos - int64(valueLen)
				it.value = data[valueStart:it.pos]
			}
			return true
		}
	}
}

func (it *sstIterator) Key() Key {
	return it.key
}

func (it *sstIterator) Value() []byte {
	return it.value
}

// sstConcatIterator iterates over a sorted list of non-overlapping SSTs as a
// single logical sorted source. Used for L1 SSTs where SSTs partition the key
// space and don't overlap, so they can be walked sequentially without merging.
type sstConcatIterator struct {
	ssts     []*sst
	openSST  func(*sst) (reader, error)
	lo       Key
	hi       Key
	sstIndex int
	current  *sstIterator
	done     bool
}

// newSSTConcatIterator creates an iterator over the given sorted, non-overlapping SSTs.
// `openSST` is a callback that returns a reader for the given SST.
func newSSTConcatIterator(ssts []*sst, lo Key, hi Key, openSST func(*sst) (reader, error)) *sstConcatIterator {
	// Find the first SST that could contain `lo`
	startIndex := 0
	if lo != nil {
		startIndex = bsearchBlock(toBlockIndices(ssts), lo) - 1
		startIndex = max(startIndex, 0)
	}

	it := &sstConcatIterator{
		ssts:     ssts,
		openSST:  openSST,
		lo:       lo,
		hi:       hi,
		sstIndex: startIndex,
	}
	it.done = startIndex >= len(ssts)
	return it
}

// toBlockIndices converts the SST list into a synthetic block index list so we
// can reuse bsearchBlock for the SST-level binary search.
func toBlockIndices(ssts []*sst) []sstBlockIndex {
	out := make([]sstBlockIndex, len(ssts))
	for i, s := range ssts {
		out[i] = sstBlockIndex{FirstKey: s.firstKey}
	}
	return out
}

func (it *sstConcatIterator) Next() bool {
	for !it.done {
		// Open the current SST iterator if needed
		if it.current == nil {
			if it.sstIndex >= len(it.ssts) {
				it.done = true
				return false
			}
			s := it.ssts[it.sstIndex]
			// Skip SSTs entirely past the upper bound
			if it.hi != nil && bytes.Compare(s.firstKey, it.hi) >= 0 {
				it.done = true
				return false
			}
			r, err := it.openSST(s)
			if err != nil {
				it.done = true
				return false
			}
			it.current = s.Iterator(it.lo, it.hi, r)
		}

		if it.current.Next() {
			return true
		}

		// Current SST exhausted; move to next
		it.current = nil
		it.sstIndex++
	}
	return false
}

func (it *sstConcatIterator) Key() Key {
	return it.current.key
}

func (it *sstConcatIterator) Value() []byte {
	return it.current.value
}

// bsearchBlock returns the index of the first block whose FirstKey > key.
// Subtract 1 to get the block that could contain the key.
func bsearchBlock(blocks []sstBlockIndex, key Key) int {
	lo, hi := 0, len(blocks)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if bytes.Compare(blocks[mid].FirstKey, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
