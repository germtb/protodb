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
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
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

Block layout (Linear Tree Block):

Each block is a "linear tree block": entries are stored in sorted
key order (linear for iteration and BulkGet), but each entry also
carries left/right child pointers forming a balanced BST (tree for
O(log N) point lookups). Same bytes, two access patterns.

The BST root is the median entry, computed via recursive-median
construction over the sorted entry slice.

┌──────────────────────────────────────────────────┐
│   entry0                                         │  ← in sorted order
│   entry1                                         │
│   ...                                            │
│   entryN-1                                       │
├──────────────────────────────────────────────────┤
│   rootOffset(u16)                                │  ← byte offset of BST root
│   entryCount(u16)                                │
│   crc32(u32)                                     │
└──────────────────────────────────────────────────┘

Entry layout:

┌──────────────────────────────────────────────────────────────┐
│   key_len(u32) | key                                         │
│   value_len(u32) | value                                     │
│   left(u16) | right(u16) | prev(u16)                         │  ← BST children + back-link, 0xFFFF = none
└──────────────────────────────────────────────────────────────┘

`prev` is the byte offset of the previous entry (ascending-key order) within
the same block, or 0xFFFF for the first entry. Lets Prev() walk backwards in
O(1) without a separately-stored offset index.

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
	cache      map[uint64]sstBlock
	blocks     []sstBlockIndex
	footer     sstFooter
	hash       string
	path       string
	fileSize   int64
	firstKey   Key
	lastKey    Key
	// clock cache reference
	referenced atomic.Bool
}

func (s *sst) clearCache() {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()
	s.cache = make(map[uint64]sstBlock)
}

type reader interface {
	ReadAt(buffer []byte, offset int64) (int, error)
	Close() error
}

// SST Constants
const Version uint16 = 1
const tombstone uint32 = 0xFFFFFFFF
const sstFooterSize int64 = 8 + 8 + 2 // BlockIndexSize + BlockCount + Version
var SSTSize int = 1024 * 1024 * 64    // 64 Mb

// Linear tree block constants
const noChild uint16 = 0xFFFF
const maxBlockEntries = 342   // ceil(4096 / 12), smallest possible entry is 12 bytes
var BlockSize int = 1024 * 4  // 4Kb
var blockFooterSize int64 = 8 // rootOffset(u16) + entryCount(u16) + crc32(u32)

type ReaderOptions struct {
	TailByteSize int64
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

// Pools for the per-SST and per-block scratch buffers. We use a fixed-slot
// channel pool, not sync.Pool — sync.Pool is cleared every couple of GC
// cycles, and the test workload triggers ~10 GC/sec, so its retention is
// effectively zero under load. The channel pool holds buffers across GCs
// because the channel itself keeps them reachable.
//
// At most one Flush and one Compact can be running at the same time
// (separate mutexes), so two slots is enough.
type bufferPool struct {
	ch  chan *bytes.Buffer
	new func() *bytes.Buffer
}

func newBufferPool(slots int, fn func() *bytes.Buffer) *bufferPool {
	return &bufferPool{ch: make(chan *bytes.Buffer, slots), new: fn}
}

func (p *bufferPool) Get() *bytes.Buffer {
	select {
	case b := <-p.ch:
		b.Reset()
		return b
	default:
		return p.new()
	}
}

func (p *bufferPool) Put(b *bytes.Buffer) {
	select {
	case p.ch <- b:
	default:
		// Pool full — let GC reclaim this buffer.
	}
}

type byteSlicePool struct {
	ch  chan *[]byte
	new func() *[]byte
}

func newByteSlicePool(slots int, fn func() *[]byte) *byteSlicePool {
	return &byteSlicePool{ch: make(chan *[]byte, slots), new: fn}
}

func (p *byteSlicePool) Get() *[]byte {
	select {
	case s := <-p.ch:
		return s
	default:
		return p.new()
	}
}

func (p *byteSlicePool) Put(s *[]byte) {
	select {
	case p.ch <- s:
	default:
	}
}

var (
	sstBufferPool = newBufferPool(2, func() *bytes.Buffer {
		b := new(bytes.Buffer)
		b.Grow(SSTSize)
		return b
	})
	sstBlockBufferPool = newBufferPool(2, func() *bytes.Buffer {
		b := new(bytes.Buffer)
		b.Grow(BlockSize)
		return b
	})
	// snappy.Encode reuses dst's backing array when it's big enough.
	// MaxEncodedLen(BlockSize) is a lower bound — blocks can grow past
	// BlockSize when they hold large entries — so the slice will get
	// re-grown by snappy on its first oversize call and the new (bigger)
	// slice sticks around in the pool.
	sstCompressedPool = newByteSlicePool(2, func() *[]byte {
		s := make([]byte, 0, snappy.MaxEncodedLen(BlockSize)+128)
		return &s
	})
)

// writeTombstones controls whether entries with value == nil are serialized.
// Flushes must write tombstones so deletions shadow older values at lower
// levels; bottom-level compactions pass false to reclaim space.
func WriteSST(fs FS, path string, entries Iterator, writeTombstones bool) ([]*sst, error) {
	var ssts []*sst
	buffer := sstBufferPool.Get()
	defer sstBufferPool.Put(buffer)
	block := sstBlockBufferPool.Get()
	defer sstBlockBufferPool.Put(block)
	compressedScratch := sstCompressedPool.Get()
	defer sstCompressedPool.Put(compressedScratch)
	var blocks []sstBlockIndex
	var inBlockEntries int = 0
	var inBlockOffsets [maxBlockEntries]uint16 // entry byte offsets within block
	var lastKey Key
	var firstKey Key
	var offset uint64 = 0

	finishBlock := func() {
		var left [maxBlockEntries]uint16
		var right [maxBlockEntries]uint16
		// Build balanced BST over the sorted entries.
		for i := range inBlockEntries {
			left[i] = noChild
			right[i] = noChild
		}
		root := buildBST(left[:inBlockEntries], right[:inBlockEntries], 0, inBlockEntries-1)

		// Patch left/right offsets. Each entry's left/right position is computed
		// from its own header — no sentinel needed, no u16 overflow risk.
		data := block.Bytes()
		for i := range inBlockEntries {
			inBlockOffset := int(inBlockOffsets[i])
			keyLen := int(binary.BigEndian.Uint32(data[inBlockOffset : inBlockOffset+4]))
			valueLen := binary.BigEndian.Uint32(data[inBlockOffset+4+keyLen : inBlockOffset+8+keyLen])
			isTombstone := valueLen == tombstone
			if isTombstone {
				valueLen = 0
			}

			leftPosition := inBlockOffset + 4 /* key_len */ + keyLen + 4 /* val_len */ + int(valueLen)
			rightPosition := leftPosition + 2
			prevPosition := rightPosition + 2

			if left[i] != noChild {
				binary.BigEndian.PutUint16(data[leftPosition:], inBlockOffsets[left[i]])
			}

			if right[i] != noChild {
				binary.BigEndian.PutUint16(data[rightPosition:], inBlockOffsets[right[i]])
			}

			// Back-link: offset of the previous entry in sorted order (i-1);
			// the first entry keeps the noChild sentinel written at entry
			// serialization time.
			if i > 0 {
				binary.BigEndian.PutUint16(data[prevPosition:], inBlockOffsets[i-1])
			}
		}

		// Footer: rootOffset(u16) + entryCount(u16) + crc32(u32)
		writeU16(block, inBlockOffsets[root])
		writeU16(block, uint16(inBlockEntries))
		writeU32(block, crc32.ChecksumIEEE(block.Bytes()))

		// Encode into the pooled scratch slice. snappy.Encode reuses dst's
		// backing array when it's large enough; we hand it our scratch and
		// reassign so subsequent Encodes keep growing it instead of
		// allocating fresh each block.
		*compressedScratch = snappy.Encode((*compressedScratch)[:0], block.Bytes())
		compressed := *compressedScratch

		blocks = append(blocks, sstBlockIndex{
			FirstKey: firstKey,
			Offset:   offset,
			Length:   uint32(len(compressed)),
		})

		buffer.Write(compressed)
		offset += uint64(len(compressed))
		inBlockEntries = 0

		// Reset preserves the backing array so the next block reuses it.
		block.Reset()
	}

	finishSST := func() error {
		if inBlockEntries > 0 {
			finishBlock()
		}

		// Write variable-length block index
		blockIndexStart := buffer.Len()
		for _, blockIndex := range blocks {
			writeU32(buffer, uint32(len(blockIndex.FirstKey)))
			buffer.Write(blockIndex.FirstKey)
			writeU64(buffer, blockIndex.Offset)
			writeU32(buffer, blockIndex.Length)
		}
		blockIndexSize := uint64(buffer.Len() - blockIndexStart)

		footer := sstFooter{
			BlockIndexSize: blockIndexSize,
			BlockCount:     uint64(len(blocks)),
			Version:        Version,
		}

		writeU64(buffer, footer.BlockIndexSize)
		writeU64(buffer, footer.BlockCount)
		writeU16(buffer, footer.Version)

		// Hash the complete SST content
		sha := sha256.Sum256(buffer.Bytes())
		hash := hex.EncodeToString(sha[:])

		// Write to temp file, rename. No per-file fsync — callers batch a single
		// syncDir(path) + manifest fsync after all SSTs in the flush/compaction
		// are written.
		tempfile, tempPath, err := CreateTempFile(fs, path, "-temp-")
		if err != nil {
			return err
		}
		renamed := false
		defer func() {
			if tempfile != nil {
				_ = tempfile.Close()
			}
			if !renamed {
				_ = fs.Remove(tempPath)
			}
		}()

		_, err = tempfile.Write(buffer.Bytes())
		if err != nil {
			return err
		}
		if err := tempfile.Close(); err != nil {
			tempfile = nil
			return err
		}
		tempfile = nil

		finalPath := filepath.Join(path, hash)
		if err := fs.Rename(tempPath, finalPath); err != nil {
			return err
		}
		renamed = true

		ssts = append(ssts, &sst{
			cache:    make(map[uint64]sstBlock),
			blocks:   blocks,
			footer:   footer,
			hash:     hash,
			path:     finalPath,
			fileSize: int64(buffer.Len()),
			firstKey: blocks[0].FirstKey,
			lastKey:  lastKey,
		})

		// Reset for next SST
		buffer.Reset()
		offset = 0
		firstKey = nil
		blocks = nil

		return nil
	}

	for entries.Next() {
		entry := entries.Current()
		key := entry.Key
		value := entry.Value

		if !writeTombstones && value == nil {
			continue
		}

		// This is to ensure entries are in sorted order
		if (len(blocks) > 0 || inBlockEntries > 0) && bytes.Compare(key, lastKey) <= 0 {
			return nil, fmt.Errorf("%w: %v <= %v", ErrUnsortedKeys, key, lastKey)
		} else {
			lastKey = key
		}

		if block.Len() > BlockSize {
			finishBlock()
		}

		// Estimate file size for SST partitioning
		fileSize := int64(offset) + sstFooterSize

		if fileSize > int64(SSTSize) {
			if err := finishSST(); err != nil {
				return nil, err
			}
		}

		if inBlockEntries == 0 {
			firstKey = append(Key(nil), key...) // copy the key
		}

		// Write entry: key_len(u32) | key | value_len(u32) | value | left(u16) | right(u16) | prev(u16)
		inBlockOffsets[inBlockEntries] = uint16(block.Len())
		writeU32(block, uint32(len(key)))
		block.Write(key)
		if value == nil {
			writeU32(block, tombstone)
		} else {
			writeU32(block, uint32(len(value)))
		}
		if value != nil {
			block.Write(value)
		}
		// left(u16) + right(u16) + prev(u16), all patched by finishBlock.
		// Default to noChild (0xFFFF) so the first entry's prev is correct
		// without needing a special case.
		writeU32(block, 0xFFFFFFFF)
		writeU16(block, 0xFFFF)

		inBlockEntries += 1
	}

	if inBlockEntries > 0 || len(blocks) > 0 {
		if err := finishSST(); err != nil {
			return nil, err
		}
	}

	return ssts, nil
}

func ReadSST(fs FS, path string, meta LevelMetadata, options *ReaderOptions) (*sst, error) {
	path = filepath.Join(path, meta.hash)
	file, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	if fileSize < sstFooterSize {
		return nil, fmt.Errorf("%w: file too small (%d bytes)", ErrCorrupted, fileSize)
	}

	footerBuf := make([]byte, sstFooterSize)
	if _, err := file.ReadAt(footerBuf, fileSize-sstFooterSize); err != nil {
		return nil, err
	}

	var footer sstFooter
	footer.BlockIndexSize = binary.BigEndian.Uint64(footerBuf[0:8])
	footer.BlockCount = binary.BigEndian.Uint64(footerBuf[8:16])
	footer.Version = binary.BigEndian.Uint16(footerBuf[16:18])

	if footer.Version != Version {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, footer.Version, Version)
	}

	blockIndexEnd := fileSize - sstFooterSize
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

	return &sst{
		cache:    make(map[uint64]sstBlock),
		blocks:   blocks,
		footer:   footer,
		hash:     meta.hash,
		path:     path,
		fileSize: fileSize,
		firstKey: meta.first,
		lastKey:  meta.last,
	}, nil
}

// GetBlock reads block `blockIndex` from `reader`. When `cache` is true the
// per-SST cache is consulted on the way in and the result populated on the
// way out. Compaction passes cache=false: it scans every block exactly
// once and would otherwise pollute the cache with megabytes of
// to-be-discarded decoded blocks (the dominant source of resident memory
// during merge work).
func (s *sst) GetBlock(blockIndex uint64, reader reader, cache bool) (*sstBlock, error) {
	if cache {
		s.cacheMutex.Lock()
		cached, ok := s.cache[blockIndex]
		s.cacheMutex.Unlock()
		if ok {
			return &cached, nil
		}
	}

	block := s.blocks[blockIndex]
	compressed := make([]byte, block.Length)
	_, err := reader.ReadAt(compressed, int64(block.Offset))
	if err != nil {
		return nil, err
	}

	data, err := snappy.Decode(nil, compressed)

	if err != nil {
		return nil, err
	}

	if len(data) < int(blockFooterSize) {
		return nil, ErrCorrupted
	}
	storedChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != storedChecksum {
		return nil, ErrCorrupted
	}

	result := sstBlock{data}
	if cache {
		s.cacheMutex.Lock()
		// Cache may have been swapped out by clearCache (engine LRU eviction);
		// the swap is racy with this Put, but worst case we orphan one decode.
		s.cache[blockIndex] = result
		s.cacheMutex.Unlock()
	}
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

	entrySize = 4 + int64(keyLen) + 4 + 6 // +6 for left(u16) + right(u16) + prev(u16)
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

	block, err := s.GetBlock(uint64(blockIdx), reader, true)
	if err != nil {
		return nil, err
	}

	// BST traversal within the block.
	data := block.data
	end := int64(len(data)) - blockFooterSize
	if end < 0 {
		return nil, ErrCorrupted
	}

	// Root offset is the first u16 in the footer.
	root := binary.BigEndian.Uint16(data[end : end+2])
	inBlockOffset := int(root)

	for inBlockOffset != int(noChild) {
		if int64(inBlockOffset)+8 > end {
			return nil, ErrCorrupted
		}
		keyLen := int(binary.BigEndian.Uint32(data[inBlockOffset : inBlockOffset+4]))
		if int64(inBlockOffset+4+keyLen+4) > end {
			return nil, ErrCorrupted
		}
		entryKey := data[inBlockOffset+4 : inBlockOffset+4+keyLen]
		valueLen := binary.BigEndian.Uint32(data[inBlockOffset+4+keyLen : inBlockOffset+8+keyLen])
		isTombstone := valueLen == tombstone
		if isTombstone {
			valueLen = 0
		}
		leftPosition := inBlockOffset + 8 + keyLen + int(valueLen)
		rightPosition := leftPosition + 2
		if int64(rightPosition+2) > end {
			return nil, ErrCorrupted
		}

		cmp := bytes.Compare(key, entryKey)
		if cmp == 0 {
			if isTombstone {
				return nil, ErrDeleted
			}
			return data[inBlockOffset+8+keyLen : inBlockOffset+8+keyLen+int(valueLen)], nil
		}
		if cmp < 0 {
			leftOff := binary.BigEndian.Uint16(data[leftPosition : leftPosition+2])
			if leftOff == noChild {
				return nil, ErrNotFound
			}
			inBlockOffset = int(leftOff)
		} else {
			rightOff := binary.BigEndian.Uint16(data[rightPosition : rightPosition+2])
			if rightOff == noChild {
				return nil, ErrNotFound
			}
			inBlockOffset = int(rightOff)
		}
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
		block, err := s.GetBlock(uint64(blockIndex), reader, true)
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
					valueStart := pos + entrySize - int64(valueLen) - 6 // -6: left+right+prev are after value
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
	current    KeyValue
	done       bool
	reverse    bool
	// cache: forwarded to GetBlock. User scans set true; compaction false.
	cache bool
}

// Iterator returns an iterator over [lo, hi) on this SST. Direction is baked
// in at construction: reverse=false iterates ascending, reverse=true
// iterates descending. `cache` decides whether decoded blocks land in the
// per-SST cache — pass false for one-shot reads (compaction).
func (s *sst) Iterator(lo Key, hi Key, r reader, reverse bool, cache bool) *sstIterator {
	it := &sstIterator{sst: s, reader: r, lo: lo, hi: hi, reverse: reverse, cache: cache}
	if reverse {
		if hi == nil {
			it.blockIndex = len(s.blocks) - 1
		} else {
			it.blockIndex = bsearchBlock(s.blocks, hi) - 1
			if it.blockIndex >= len(s.blocks) {
				it.blockIndex = len(s.blocks) - 1
			}
		}
		it.done = it.blockIndex < 0
	} else {
		it.blockIndex = max(bsearchBlock(s.blocks, lo)-1, 0)
		it.done = it.blockIndex >= len(s.blocks)
	}
	return it
}

func findLastEntryOffset(data []byte) (int64, error) {
	entriesEnd := int64(len(data)) - blockFooterSize
	if entriesEnd < 2 {
		return 0, ErrCorrupted
	}
	prevOfLast := binary.BigEndian.Uint16(data[entriesEnd-2 : entriesEnd])
	if prevOfLast == noChild {
		// Single-entry block: the only entry starts at 0.
		return 0, nil
	}
	_, _, prevEntrySize, err := readEntry(data, int64(prevOfLast))
	if err != nil {
		return 0, err
	}
	return int64(prevOfLast) + prevEntrySize, nil
}

func (it *sstIterator) Next() bool {
	if it.reverse {
		return it.nextReverse()
	} else {
		return it.nextForward()
	}
}

func (it *sstIterator) nextForward() bool {
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
			block, err := it.sst.GetBlock(uint64(it.blockIndex), it.reader, it.cache)
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
			if valueLen == tombstone {
				it.current = KeyValue{entryKey, nil}
			} else {
				valueStart := it.pos - int64(valueLen) - 6 // -6: left+right+prev are after value
				it.current = KeyValue{entryKey, data[valueStart : valueStart+int64(valueLen)]}
			}
			return true
		}
	}
}

func (it *sstIterator) nextReverse() bool {
	if it.done {
		return false
	}
	for {
		// Load block if needed; position at its last entry.
		if it.block == nil {
			if it.blockIndex < 0 {
				it.done = true
				return false
			}
			block, err := it.sst.GetBlock(uint64(it.blockIndex), it.reader, it.cache)
			if err != nil {
				it.done = true
				return false
			}
			it.block = block
			it.pos, err = findLastEntryOffset(block.data)
			if err != nil {
				it.done = true
				return false
			}
		}

		data := it.block.data
		cur := it.pos
		entryKey, valueLen, entrySize, err := readEntry(data, cur)
		if err != nil {
			it.done = true
			return false
		}

		// Advance the cursor: follow this entry's prev pointer (last 2 bytes
		// of the entry). noChild means we just read the first entry of this
		// block — drop into the previous block on the next call.
		prevOffset := binary.BigEndian.Uint16(data[cur+entrySize-2 : cur+entrySize])
		if prevOffset == noChild {
			it.blockIndex--
			it.block = nil
		} else {
			it.pos = int64(prevOffset)
		}

		// Entries in a block are in ascending key order. Walking backward,
		// keys too high for the range come first — skip them, keep going.
		// A key below lo means we've left the range entirely — done.
		if it.hi != nil && bytes.Compare(entryKey, it.hi) >= 0 {
			continue
		}
		if it.lo != nil && bytes.Compare(entryKey, it.lo) < 0 {
			it.done = true
			return false
		}

		if valueLen == tombstone {
			it.current = KeyValue{entryKey, nil}
		} else {
			valueStart := cur + 4 + int64(len(entryKey)) + 4
			it.current = KeyValue{entryKey, data[valueStart : valueStart+int64(valueLen)]}
		}
		return true
	}
}

func (it *sstIterator) Current() KeyValue {
	return it.current
}

func (it *sstIterator) Close() error {
	if it.reader == nil {
		return nil
	}
	err := it.reader.Close()
	it.reader = nil // guard against double-close
	return err
}

// sstConcatIterator iterates over a sorted list of non-overlapping SSTs as a
// single logical sorted source. Used for L1 SSTs where SSTs partition the key
// space and don't overlap, so they can be walked sequentially without merging.
type sstConcatIterator struct {
	ssts            []*sst
	openSST         func(*sst) (reader, error)
	lo              Key
	hi              Key
	sstIndex        int
	currentIterator *sstIterator
	done            bool
	reverse         bool // determines stepping direction
	cache           bool // forwarded to per-sst Iterator
}

// newSSTConcatIterator creates an iterator over the given sorted,
// non-overlapping SSTs, in either direction. `openSST` returns a reader for
// the given SST. `cache` is forwarded to each underlying sst.Iterator.
func newSSTConcatIterator(ssts []*sst, lo Key, hi Key, openSST func(*sst) (reader, error), reverse bool, cache bool) *sstConcatIterator {
	var startIndex int
	if reverse {
		if hi == nil {
			startIndex = len(ssts) - 1
		} else {
			startIndex = bsearchBlock(toBlockIndices(ssts), hi) - 1
			if startIndex >= len(ssts) {
				startIndex = len(ssts) - 1
			}
		}
	} else {
		if lo != nil {
			startIndex = bsearchBlock(toBlockIndices(ssts), lo) - 1
			startIndex = max(startIndex, 0)
		}
	}

	it := &sstConcatIterator{
		ssts:     ssts,
		openSST:  openSST,
		lo:       lo,
		hi:       hi,
		sstIndex: startIndex,
		reverse:  reverse,
		cache:    cache,
	}
	if reverse {
		it.done = startIndex < 0
	} else {
		it.done = startIndex >= len(ssts)
	}
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

// Next walks one step in the configured direction, transitioning between
// sub-iterators at SST boundaries.
func (it *sstConcatIterator) Next() bool {
	for !it.done {
		if it.currentIterator == nil {
			if it.reverse {
				if it.sstIndex < 0 {
					it.done = true
					return false
				}
			} else {
				if it.sstIndex >= len(it.ssts) {
					it.done = true
					return false
				}
			}
			s := it.ssts[it.sstIndex]
			// Skip SSTs entirely outside the bound in the direction of travel.
			if it.reverse {
				if it.lo != nil && s.lastKey != nil && bytes.Compare(s.lastKey, it.lo) < 0 {
					it.done = true
					return false
				}
			} else {
				if it.hi != nil && bytes.Compare(s.firstKey, it.hi) >= 0 {
					it.done = true
					return false
				}
			}
			r, err := it.openSST(s)
			if err != nil {
				it.done = true
				return false
			}
			it.currentIterator = s.Iterator(it.lo, it.hi, r, it.reverse, it.cache)
		}

		if it.currentIterator.Next() {
			return true
		}

		// Current SST exhausted in this direction; move to next/prev SST.
		// TODO: capture iterator close error.
		_ = it.currentIterator.Close()
		it.currentIterator = nil
		if it.reverse {
			it.sstIndex--
		} else {
			it.sstIndex++
		}
	}
	return false
}

func (it *sstConcatIterator) Current() KeyValue {
	return it.currentIterator.current
}

func (it *sstConcatIterator) Close() error {
	if it.currentIterator != nil {
		return it.currentIterator.Close()
	}
	return nil
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

func buildBST(left []uint16, right []uint16, lo int, hi int) int {
	if lo > hi {
		return -1
	}
	mid := lo + (hi-lo)/2
	l := buildBST(left, right, lo, mid-1)
	r := buildBST(left, right, mid+1, hi)
	if l != -1 {
		left[mid] = uint16(l)
	}
	if r != -1 {
		right[mid] = uint16(r)
	}
	return mid
}
