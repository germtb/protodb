// Command bench runs a fixed set of head-to-head workloads against
// protodb's LSM engine and a few common embedded key-value stores
// (Pebble, SQLite, BoltDB) and prints a side-by-side comparison table.
//
// Why a binary instead of `go test -bench`: we don't need the testing
// framework's b.N calibration (most of our workloads have heavy setup
// where b.N scaling fights us), benchstat statistics, or _test.go file
// naming. A small driver gives us direct control over the budget,
// memory measurement, and output format.
//
// All writes use per-commit fsync — the only durability profile that
// matters in production. Workloads are seeded so every backend sees the
// same key/value byte stream.
//
// Usage:
//
//	go run ./benchmark
//	go run ./benchmark -bench=Get
//	go run ./benchmark -bench=Get
//	go run ./benchmark -budget=2s -bench='Concurrent.*'
package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	_ "github.com/mattn/go-sqlite3"
	bolt "go.etcd.io/bbolt"

	"github.com/germtb/protodb"
)

// =============================================================================
// Backend abstraction.
// =============================================================================

// DB is the minimum surface every backend exposes. The `sync` flag on
// mutating methods picks per-commit fsync (true) or kernel-page-cache
// only (false). Population uses sync=false to keep setup fast; benches
// pass sync=true for measured ops.
type DB interface {
	Put(key, value []byte, sync bool) error
	// BulkPut writes a batch of entries in one transaction. Used by
	// populate (sync=false) and the BatchedWrites benchmark (sync=true).
	BulkPut(entries []entry, sync bool) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte, sync bool) error
	Scan(lo, hi []byte) Iterator
	ReverseScan(lo, hi []byte) Iterator
	Compact(lo, hi []byte) error
	Close() error
}

type Iterator interface {
	Next() bool
	Close() error
}

// --- LSM (protodb) ---

type lsmDB struct{ eng *protodb.Engine }

// benchLSMBlockCacheBytes matches Pebble's cache below so the comparison
// is symmetric: both engines hold up to 128 MB of decoded block content.
const benchLSMBlockCacheBytes int64 = 128 << 20

func openLSM(dir string) (DB, error) {
	eng, err := protodb.Open(dir, protodb.WithBlockCacheSize(benchLSMBlockCacheBytes))
	if err != nil {
		return nil, err
	}
	return &lsmDB{eng: eng}, nil
}

func (d *lsmDB) Put(k, v []byte, sync bool) error {
	d.setSync(sync)
	return d.eng.Put(k, v)
}

func (d *lsmDB) BulkPut(entries []entry, sync bool) error {
	d.setSync(sync)
	tx := d.eng.Transaction()
	for _, e := range entries {
		tx.Put(e.key, e.value)
	}
	return tx.Commit()
}
func (d *lsmDB) Get(k []byte) ([]byte, error) { return d.eng.Get(k) }
func (d *lsmDB) Delete(k []byte, sync bool) error {
	d.setSync(sync)
	return d.eng.Delete(k)
}
func (d *lsmDB) Scan(lo, hi []byte) Iterator        { return lsmIter{d.eng.Scan(lo, hi)} }
func (d *lsmDB) ReverseScan(lo, hi []byte) Iterator { return lsmIter{d.eng.ReverseScan(lo, hi)} }
func (d *lsmDB) Compact(lo, hi []byte) error        { return d.eng.Compact() }
func (d *lsmDB) Close() error                       { return d.eng.Close() }

// setSync flips Policy.Sync without copying the rest of the policy.
// We allocate one Policy on first call and reuse it; SetPolicy stores
// the pointer so further field flips need no extra allocation.
func (d *lsmDB) setSync(sync bool) {
	d.eng.SetPolicy(&protodb.Policy{
		SoftCompactionThreshold: 4,
		HardCompactionThreshold: 16,
		FlushThreshold:          1024 * 1024 * 64,
		Sync:                    sync,
	})
}

type lsmIter struct{ it protodb.Iterator }

func (i lsmIter) Next() bool   { return i.it.Next() }
func (i lsmIter) Close() error { return i.it.Close() }

// --- Pebble ---

type pebbleDB struct{ db *pebble.DB }

// benchPebbleCacheBytes matches the LSM CLOCK cache budget (2 × SSTSize
// = 128 MB). Pebble's default is 8 MB, far smaller than what production
// deployments configure (CockroachDB sets ~25% of system RAM). At 128 MB
// both engines have the same memory budget, and the 500 MB Loaded/*
// working set forces real cache eviction on both sides.
const benchPebbleCacheBytes = 128 << 20

func openPebble(dir string) (DB, error) {
	cache := pebble.NewCache(benchPebbleCacheBytes)
	defer cache.Unref()
	db, err := pebble.Open(filepath.Join(dir, "pebble"), &pebble.Options{
		Cache:           cache,
		MemTableSize:    64 * 1024 * 1024,
		BytesPerSync:    0,
		WALBytesPerSync: 0,
		// Suppress Pebble's WAL replay / housekeeping log lines so they
		// don't intermix with our table output.
		Logger: discardLogger{},
	})
	if err != nil {
		return nil, err
	}
	return &pebbleDB{db: db}, nil
}

type discardLogger struct{}

func (discardLogger) Infof(format string, args ...any)  {}
func (discardLogger) Errorf(format string, args ...any) {}
func (discardLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func writeOpts(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}
	return pebble.NoSync
}

func (d *pebbleDB) Put(k, v []byte, sync bool) error { return d.db.Set(k, v, writeOpts(sync)) }

func (d *pebbleDB) BulkPut(entries []entry, sync bool) error {
	batch := d.db.NewBatch()
	for _, e := range entries {
		if err := batch.Set(e.key, e.value, nil); err != nil {
			batch.Close()
			return err
		}
	}
	return batch.Commit(writeOpts(sync))
}

func (d *pebbleDB) Get(k []byte) ([]byte, error) {
	v, closer, err := d.db.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, nil
}

func (d *pebbleDB) Delete(k []byte, sync bool) error { return d.db.Delete(k, writeOpts(sync)) }

func (d *pebbleDB) Scan(lo, hi []byte) Iterator {
	it, _ := d.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	return &pebbleIter{it: it, started: false, reverse: false}
}

func (d *pebbleDB) ReverseScan(lo, hi []byte) Iterator {
	it, _ := d.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	return &pebbleIter{it: it, started: false, reverse: true}
}

func (d *pebbleDB) Compact(lo, hi []byte) error { return d.db.Compact(lo, hi, true) }
func (d *pebbleDB) Close() error                { return d.db.Close() }

type pebbleIter struct {
	it      *pebble.Iterator
	started bool
	reverse bool
}

func (i *pebbleIter) Next() bool {
	if !i.started {
		i.started = true
		if i.reverse {
			return i.it.Last()
		}
		return i.it.First()
	}
	if i.reverse {
		return i.it.Prev()
	}
	return i.it.Next()
}

func (i *pebbleIter) Close() error { return i.it.Close() }

// --- SQLite ---

type sqliteDB struct {
	db   *sql.DB
	put  *sql.Stmt
	get  *sql.Stmt
	del  *sql.Stmt
	scan *sql.Stmt
	rev  *sql.Stmt
}

func openSQLite(dir string) (DB, error) {
	db, err := sql.Open("sqlite3", filepath.Join(dir, "sqlite.db"))
	if err != nil {
		return nil, err
	}
	// Pin to a single connection. Without this, database/sql's pool will
	// hand out connections that haven't seen our PRAGMA — so our
	// `synchronous=FULL` setting only sticks to one of them and
	// subsequent INSERTs run with whatever default the others were
	// opened with. That bug made earlier benchmark runs report SQLite
	// writes at ~80µs (i.e. NoSync) instead of the expected ~3ms.
	db.SetMaxOpenConns(1)
	for _, p := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=FULL`,
		// On macOS, libc fsync(2) only flushes the kernel page cache —
		// not the disk. Go's os.File.Sync() (used by protodb LSM, Pebble,
		// Bolt) silently upgrades to F_FULLFSYNC, which forces an actual
		// platter flush at ~3ms per call. PRAGMA fullfsync=true makes
		// SQLite do the same so the comparison is on equal durability
		// terms; without it SQLite reports ~50µs per Sync write because
		// it isn't really syncing.
		`PRAGMA fullfsync=true`,
		`CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB) WITHOUT ROWID`,
	} {
		if _, err := db.Exec(p); err != nil {
			return nil, err
		}
	}
	put, _ := db.Prepare(`INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
	get, _ := db.Prepare(`SELECT value FROM kv WHERE key = ?`)
	del, _ := db.Prepare(`DELETE FROM kv WHERE key = ?`)
	scan, _ := db.Prepare(`SELECT key, value FROM kv WHERE key >= ? AND key < ? ORDER BY key`)
	rev, _ := db.Prepare(`SELECT key, value FROM kv WHERE key >= ? AND key < ? ORDER BY key DESC`)
	return &sqliteDB{db: db, put: put, get: get, del: del, scan: scan, rev: rev}, nil
}

func (d *sqliteDB) Put(k, v []byte, sync bool) error {
	d.setSync(sync)
	_, err := d.put.Exec(k, v)
	return err
}

func (d *sqliteDB) BulkPut(entries []entry, sync bool) error {
	d.setSync(sync)
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	stmt := tx.Stmt(d.put)
	for _, e := range entries {
		if _, err := stmt.Exec(e.key, e.value); err != nil {
			tx.Rollback()
			return err
		}
	}
	stmt.Close()
	return tx.Commit()
}

func (d *sqliteDB) Get(k []byte) ([]byte, error) {
	var v []byte
	err := d.get.QueryRow(k).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (d *sqliteDB) Delete(k []byte, sync bool) error {
	d.setSync(sync)
	_, err := d.del.Exec(k)
	return err
}

func (d *sqliteDB) Scan(lo, hi []byte) Iterator {
	rows, err := d.scan.Query(lo, hi)
	return &sqliteIter{rows: rows, err: err}
}

func (d *sqliteDB) ReverseScan(lo, hi []byte) Iterator {
	rows, err := d.rev.Query(lo, hi)
	return &sqliteIter{rows: rows, err: err}
}

// SQLite has no manual compact; returning nil is the closest-match no-op.
func (d *sqliteDB) Compact(lo, hi []byte) error { return nil }
func (d *sqliteDB) Close() error                { return d.db.Close() }

// setSync toggles SQLite synchronous mode at runtime. NORMAL = WAL
// fsynced lazily at checkpoints; FULL = fsync per commit.
func (d *sqliteDB) setSync(sync bool) {
	mode := "NORMAL"
	if sync {
		mode = "FULL"
	}
	_, _ = d.db.Exec("PRAGMA synchronous=" + mode)
}

type sqliteIter struct {
	rows *sql.Rows
	err  error
}

func (i *sqliteIter) Next() bool {
	if i.err != nil || i.rows == nil {
		return false
	}
	if !i.rows.Next() {
		return false
	}
	var k, v []byte
	if err := i.rows.Scan(&k, &v); err != nil {
		i.err = err
		return false
	}
	return true
}

func (i *sqliteIter) Close() error {
	if i.rows == nil {
		return i.err
	}
	return i.rows.Close()
}

// --- BoltDB ---

var boltBucket = []byte("kv")

type boltDB struct{ db *bolt.DB }

func openBolt(dir string) (DB, error) {
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &boltDB{db: db}, nil
}

func (d *boltDB) Put(k, v []byte, sync bool) error {
	d.db.NoSync = !sync
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Put(k, v)
	})
}

func (d *boltDB) BulkPut(entries []entry, sync bool) error {
	d.db.NoSync = !sync
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		for _, e := range entries {
			if err := bucket.Put(e.key, e.value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *boltDB) Get(k []byte) (out []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(boltBucket).Get(k)
		if v != nil {
			out = append([]byte(nil), v...)
		}
		return nil
	})
	return out, err
}

func (d *boltDB) Delete(k []byte, sync bool) error {
	d.db.NoSync = !sync
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(boltBucket).Delete(k)
	})
}

func (d *boltDB) Scan(lo, hi []byte) Iterator {
	return &boltIter{db: d.db, lo: lo, hi: hi, reverse: false}
}

func (d *boltDB) ReverseScan(lo, hi []byte) Iterator {
	return &boltIter{db: d.db, lo: lo, hi: hi, reverse: true}
}

// Bolt has no compaction (B+ tree).
func (d *boltDB) Compact(lo, hi []byte) error { return nil }
func (d *boltDB) Close() error                { return d.db.Close() }

// boltIter materializes the range up front — Bolt iteration must run
// inside a View txn, but our Iterator interface lets the caller drive
// Next from outside. Materializing is simpler than juggling tx lifetimes;
// this is benchmark code, not a hot path.
type boltIter struct {
	keys    [][]byte
	idx     int
	reverse bool
	db      *bolt.DB
	lo, hi  []byte
	loaded  bool
}

func (i *boltIter) Next() bool {
	if !i.loaded {
		i.loaded = true
		i.db.View(func(tx *bolt.Tx) error {
			cur := tx.Bucket(boltBucket).Cursor()
			for k, _ := cur.Seek(i.lo); k != nil && bytes.Compare(k, i.hi) < 0; k, _ = cur.Next() {
				i.keys = append(i.keys, append([]byte(nil), k...))
			}
			return nil
		})
		if i.reverse {
			for l, r := 0, len(i.keys)-1; l < r; l, r = l+1, r-1 {
				i.keys[l], i.keys[r] = i.keys[r], i.keys[l]
			}
		}
		i.idx = -1
	}
	i.idx++
	return i.idx < len(i.keys)
}

func (i *boltIter) Close() error { return nil }

// =============================================================================
// Workload generation.
// =============================================================================

type entry struct {
	key   []byte
	value []byte
}

func uint64Key(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

// fillRandom populates b with pseudo-random bytes from rng. Substitute
// for math/rand/v2.Rand which doesn't expose Read directly.
func fillRandom(rng *mathrand.Rand, b []byte) {
	for i := 0; i < len(b); i += 8 {
		u := rng.Uint64()
		end := i + 8
		if end > len(b) {
			end = len(b)
		}
		for j := i; j < end; j++ {
			b[j] = byte(u)
			u >>= 8
		}
	}
}

// genWorkload produces n entries with sequential uint64 keys and random
// value content sized 100–2000 bytes. Same seed → same byte stream → all
// backends see identical input.
func genWorkload(n int, seed uint64) []entry {
	rng := mathrand.New(mathrand.NewPCG(seed, 0))
	out := make([]entry, n)
	for i := range out {
		out[i].key = uint64Key(uint64(i))
		sz := 100 + rng.IntN(1900)
		v := make([]byte, sz)
		fillRandom(rng, v)
		out[i].value = v
	}
	return out
}

// genReadIndices produces n indices in [0, populateSize) seeded by `seed`.
func genReadIndices(n, populateSize int, seed uint64) []int {
	rng := mathrand.New(mathrand.NewPCG(seed, 0))
	out := make([]int, n)
	for i := range out {
		out[i] = rng.IntN(populateSize)
	}
	return out
}

// populate writes entries in chunks via BulkPut(sync=false). ~30× faster
// than per-Put with autocommit on SQLite/Bolt; meaningfully faster on
// LSM/Pebble too because of batched WAL writes.
func populate(db DB, entries []entry) error {
	const chunk = 10_000
	for start := 0; start < len(entries); start += chunk {
		end := start + chunk
		if end > len(entries) {
			end = len(entries)
		}
		if err := db.BulkPut(entries[start:end], false); err != nil {
			return err
		}
	}
	return nil
}

// =============================================================================
// On-disk cache for "loaded" benchmarks.
//
// Populating ~500MB into a fresh DB takes 30-60 seconds. Caching the
// populated state on disk lets repeat runs skip straight to measurement.
// Each (cacheKey, backend) gets its own dir under the user cache root;
// when the .populated stamp is present we reuse the dir as-is.
//
// Invalidate by removing the cache root manually, or bump cacheVersion.
// =============================================================================

const cacheVersion = "v1"

func cacheRoot() string {
	root, err := os.UserCacheDir()
	if err != nil || root == "" {
		root = filepath.Join(os.TempDir(), "protodb-bench-cache")
	}
	return filepath.Join(root, "protodb-bench", cacheVersion)
}

func cacheDir(key, backend string) string {
	return filepath.Join(cacheRoot(), key+"-"+backend)
}

func isPopulated(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, ".populated"))
	return err == nil
}

func markPopulated(dir string) error {
	return os.WriteFile(filepath.Join(dir, ".populated"), []byte("ok\n"), 0644)
}

// =============================================================================
// Benchmarks.
// =============================================================================

const (
	dataSeed     = 0xc0ffee
	indexSeed    = 0xbeef
	populateSize = 10_000
	scanBatch    = 1000
)

// BenchFunc runs the measured phase. It returns ops completed and any
// fatal error. The driver provides a wall-time budget; the bench loops
// until the budget elapses.
type BenchFunc func(db DB, budget time.Duration) (int, error)

// Bench bundles a name, the backends it runs on, and the measured
// function. If Backends is empty the bench runs on every registered
// backend; if non-empty only the listed ones run.
//
// CacheKey, when non-empty, makes the driver open the DB at the
// pre-populated cache dir keyed by (CacheKey, backend) instead of a
// fresh tempdir. The cache must exist on disk — run
// `go run ./benchmark populate` first. Used by the Loaded/* benches
// that measure performance against a ~500MB pre-loaded DB.
type Bench struct {
	Name     string
	Backends []string
	CacheKey string
	Run      BenchFunc
}

// loopFor calls fn until the budget elapses, returning the number of
// successful invocations.
func loopFor(budget time.Duration, fn func(i int) error) (int, error) {
	deadline := time.Now().Add(budget)
	n := 0
	for time.Now().Before(deadline) {
		if err := fn(n); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func benchWrite(db DB, budget time.Duration) (int, error) {
	w := genWorkload(populateSize, dataSeed)
	return loopFor(budget, func(i int) error {
		return db.Put(uint64Key(uint64(i)), w[i%len(w)].value, true)
	})
}

func benchGet(db DB, budget time.Duration) (int, error) {
	w := genWorkload(populateSize, dataSeed)
	if err := populate(db, w); err != nil {
		return 0, err
	}
	idx := genReadIndices(100_000, populateSize, indexSeed)
	return loopFor(budget, func(i int) error {
		_, err := db.Get(uint64Key(uint64(idx[i%len(idx)])))
		return err
	})
}

func benchGetMiss(db DB, budget time.Duration) (int, error) {
	// Populate even keys; query odd keys.
	w := genWorkload(populateSize, dataSeed)
	for i := range w {
		w[i].key = uint64Key(uint64(i * 2))
	}
	if err := populate(db, w); err != nil {
		return 0, err
	}
	return loopFor(budget, func(i int) error {
		_, _ = db.Get(uint64Key(uint64(i*2 + 1)))
		return nil
	})
}

func benchScan(db DB, budget time.Duration) (int, error) {
	w := genWorkload(populateSize*10, dataSeed)
	if err := populate(db, w); err != nil {
		return 0, err
	}
	starts := genReadIndices(10_000, populateSize*10-scanBatch, indexSeed)
	return loopFor(budget, func(i int) error {
		s := starts[i%len(starts)]
		it := db.Scan(uint64Key(uint64(s)), uint64Key(uint64(s+scanBatch)))
		for it.Next() {
		}
		return it.Close()
	})
}

func benchReverseScan(db DB, budget time.Duration) (int, error) {
	w := genWorkload(populateSize*10, dataSeed)
	if err := populate(db, w); err != nil {
		return 0, err
	}
	starts := genReadIndices(10_000, populateSize*10-scanBatch, indexSeed)
	return loopFor(budget, func(i int) error {
		s := starts[i%len(starts)]
		it := db.ReverseScan(uint64Key(uint64(s)), uint64Key(uint64(s+scanBatch)))
		for it.Next() {
		}
		return it.Close()
	})
}

// concurrentWriters spawns n writer goroutines, each putting against
// non-overlapping key ranges, until the budget elapses. Returns total
// puts.
func concurrentWriters(n int) BenchFunc {
	return func(db DB, budget time.Duration) (int, error) {
		w := genWorkload(populateSize, dataSeed)
		stop := make(chan struct{})
		time.AfterFunc(budget, func() { close(stop) })

		var total atomic.Int64
		var wg sync.WaitGroup
		for w0 := 0; w0 < n; w0++ {
			wg.Add(1)
			go func(base int) {
				defer wg.Done()
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
						k := uint64Key(uint64(base + i*n))
						if err := db.Put(k, w[i%len(w)].value, true); err != nil {
							return
						}
						i++
						total.Add(1)
					}
				}
			}(w0)
		}
		wg.Wait()
		return int(total.Load()), nil
	}
}

func concurrentReaders(n int) BenchFunc {
	return func(db DB, budget time.Duration) (int, error) {
		w := genWorkload(populateSize, dataSeed)
		if err := populate(db, w); err != nil {
			return 0, err
		}
		idx := genReadIndices(100_000, populateSize, indexSeed)
		stop := make(chan struct{})
		time.AfterFunc(budget, func() { close(stop) })

		var total atomic.Int64
		var wg sync.WaitGroup
		for r := 0; r < n; r++ {
			wg.Add(1)
			go func(off int) {
				defer wg.Done()
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
						_, err := db.Get(uint64Key(uint64(idx[(off+i)%len(idx)])))
						if err != nil {
							return
						}
						i++
						total.Add(1)
					}
				}
			}(r * len(idx) / n)
		}
		wg.Wait()
		return int(total.Load()), nil
	}
}

// concurrentReadWrites spawns n goroutines each running a 50/50 Get/Put
// stream against the populated keyspace (writes overwrite existing keys).
// Reported ns/op is total time divided by total ops (reads + writes), so
// it's a true blended latency rather than a read-dominated average.
func concurrentReadWrites(n int) BenchFunc {
	return concurrentMixedStream(n, populateSize, false)
}

func loadedConcurrentReadWrites(n int) BenchFunc {
	return concurrentMixedStream(n, loadedSize, true)
}

func concurrentMixedStream(n, size int, preloaded bool) BenchFunc {
	return func(db DB, budget time.Duration) (int, error) {
		w := genWorkload(size, dataSeed)
		if !preloaded {
			if err := populate(db, w); err != nil {
				return 0, err
			}
		}
		idx := genReadIndices(100_000, size, indexSeed)
		stop := make(chan struct{})
		time.AfterFunc(budget, func() { close(stop) })

		var total atomic.Int64
		var wg sync.WaitGroup
		for g := 0; g < n; g++ {
			wg.Add(1)
			go func(seed uint64) {
				defer wg.Done()
				rng := mathrand.New(mathrand.NewPCG(seed, seed+1))
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
						k := uint64Key(uint64(idx[i%len(idx)]))
						if rng.IntN(2) == 0 {
							if _, err := db.Get(k); err != nil {
								return
							}
						} else {
							if err := db.Put(k, w[i%len(w)].value, true); err != nil {
								return
							}
						}
						i++
						total.Add(1)
					}
				}
			}(uint64(g))
		}
		wg.Wait()
		return int(total.Load()), nil
	}
}

// loadedConcurrentReaders is concurrentReaders against the pre-populated
// loaded dataset (cache-resident assumption broken — working set exceeds
// the block cache, so this measures cold-ish reads under contention).
func loadedConcurrentReaders(n int) BenchFunc {
	return func(db DB, budget time.Duration) (int, error) {
		idx := genReadIndices(100_000, loadedSize, indexSeed)
		stop := make(chan struct{})
		time.AfterFunc(budget, func() { close(stop) })

		var total atomic.Int64
		var wg sync.WaitGroup
		for r := 0; r < n; r++ {
			wg.Add(1)
			go func(off int) {
				defer wg.Done()
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
						_, err := db.Get(uint64Key(uint64(idx[(off+i)%len(idx)])))
						if err != nil {
							return
						}
						i++
						total.Add(1)
					}
				}
			}(r * len(idx) / n)
		}
		wg.Wait()
		return int(total.Load()), nil
	}
}

// benchBatchedWrites commits batches of `batchSize` entries in a single
// transaction with sync=true. The reported ns/op is per-entry, so dividing
// by avg value size gives MB/s. Larger batchSize amortizes the fsync cost;
// throughput should grow until disk bandwidth caps it.
func benchBatchedWrites(batchSize int) BenchFunc {
	return func(db DB, budget time.Duration) (int, error) {
		w := genWorkload(batchSize, dataSeed)
		stop := make(chan struct{})
		time.AfterFunc(budget, func() { close(stop) })
		entries := 0
		offset := uint64(0)
		for {
			select {
			case <-stop:
				return entries, nil
			default:
				for i := range w {
					w[i].key = uint64Key(offset + uint64(i))
				}
				if err := db.BulkPut(w, true); err != nil {
					return entries, err
				}
				entries += batchSize
				offset += uint64(batchSize)
			}
		}
	}
}

func benchSustainedWrites(db DB, budget time.Duration) (int, error) {
	w := genWorkload(populateSize, dataSeed)
	return loopFor(budget, func(i int) error {
		return db.Put(uint64Key(uint64(i)), w[i%len(w)].value, true)
	})
}

// benchCompaction populates n entries then times a single Compact() over
// the entire range. The whole flow is timed inside the budget — `ops`
// counts populate+compact passes. Per-iteration setup is heavy, so use
// `-budget=10s+` for big n.
func benchCompaction(n int) BenchFunc {
	return func(db DB, budget time.Duration) (int, error) {
		w := genWorkload(n, dataSeed)
		hi := uint64Key(uint64(n + 1))
		ops := 0
		deadline := time.Now().Add(budget)
		for time.Now().Before(deadline) {
			for _, e := range w {
				if err := db.Put(e.key, e.value, false); err != nil {
					return ops, err
				}
			}
			if err := db.Compact(uint64Key(0), hi); err != nil {
				return ops, err
			}
			ops++
		}
		return ops, nil
	}
}

// benchMixedWorkload runs a seeded random op stream against the engine.
// Op mix is read-heavy (90% Get / 5% Put / 1% Delete / 4% Scan) — a
// rough match for typical production read/write ratios. With Sync mode,
// even 5% Puts add a ~150µs floor per op on average; pushing writes
// higher just makes this a fsync benchmark. If you want a write-heavy
// trace, run BenchmarkSustainedWrites or ConcurrentWrites instead.
//
// mixedPopulateSize is intentionally larger than the default populateSize
// so the working set exceeds the 128 MB block cache — otherwise every Get
// would be a cache hit on a bench-tuned engine, and the test would only
// measure Put/Delete/Scan latency.
func benchMixedWorkload(db DB, budget time.Duration) (int, error) {
	const mixedPopulateSize = 200_000 // ~200 MB at avg 1 KB value, exceeds 128 MB cache
	w := genWorkload(mixedPopulateSize, dataSeed)
	if err := populate(db, w); err != nil {
		return 0, err
	}

	type op struct {
		kind  byte
		key   uint64
		value []byte
	}
	rng := mathrand.New(mathrand.NewPCG(indexSeed, 0))
	const scriptSize = 100_000
	script := make([]op, scriptSize)
	for i := range script {
		r := rng.IntN(100)
		key := uint64(rng.IntN(mixedPopulateSize * 2))
		switch {
		case r < 90:
			script[i] = op{kind: 'g', key: key}
		case r < 95:
			sz := 100 + rng.IntN(1900)
			v := make([]byte, sz)
			fillRandom(rng, v)
			script[i] = op{kind: 'p', key: key, value: v}
		case r < 96:
			script[i] = op{kind: 'd', key: key}
		default:
			script[i] = op{kind: 's', key: key}
		}
	}

	return loopFor(budget, func(i int) error {
		o := script[i%len(script)]
		k := uint64Key(o.key)
		switch o.kind {
		case 'g':
			_, err := db.Get(k)
			return err
		case 'p':
			return db.Put(k, o.value, true)
		case 'd':
			return db.Delete(k, true)
		case 's':
			it := db.Scan(k, uint64Key(o.key+uint64(scanBatch)))
			for it.Next() {
			}
			return it.Close()
		}
		return nil
	})
}

// =============================================================================
// Bench registry.
// =============================================================================

func benches() []Bench {
	var out []Bench

	out = append(out,
		Bench{Name: "Write", Run: benchWrite},
		Bench{Name: "Get", Run: benchGet},
		Bench{Name: "GetMiss", Backends: []string{"LSM", "Pebble", "Bolt"}, Run: benchGetMiss},
		Bench{Name: "Scan", Run: benchScan},
		Bench{Name: "ReverseScan", Run: benchReverseScan},
		Bench{Name: "SustainedWrites", Backends: []string{"LSM", "Pebble"}, Run: benchSustainedWrites},
	)

	for _, n := range []int{1, 10, 100, 1000, 10000} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("BatchedWrites/%d", n),
			Backends: []string{"LSM", "Pebble", "SQLite", "Bolt"},
			Run:      benchBatchedWrites(n),
		})
	}

	out = append(out,
		Bench{Name: "MixedWorkload", Backends: []string{"LSM", "Pebble"}, Run: benchMixedWorkload},
	)

	for _, n := range []int{1, 2, 4, 8, 16} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("ConcurrentWrites/%dw", n),
			Backends: []string{"LSM", "Pebble"},
			Run:      concurrentWriters(n),
		})
	}
	for _, n := range []int{1, 2, 4, 8, 16} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("ConcurrentReads/%dr", n),
			Backends: []string{"LSM", "Pebble"},
			Run:      concurrentReaders(n),
		})
	}
	for _, n := range []int{2, 4, 8, 16} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("ConcurrentReadWrites/%dg", n),
			Backends: []string{"LSM", "Pebble"},
			Run:      concurrentReadWrites(n),
		})
	}
	for _, n := range []int{10, 100, 1000, 10000} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("Compaction/%d", n),
			Backends: []string{"LSM", "Pebble"},
			Run:      benchCompaction(n),
		})
	}

	// Loaded variants: same read paths, but against a ~500MB pre-populated
	// DB (loadedSize entries). The cache lives on disk under
	// $XDG_CACHE_HOME/protodb-bench/v1/loaded-500K-<backend>; populate it
	// once with `go run ./benchmark populate`. Driver errors out if the
	// cache is missing.
	out = append(out,
		Bench{Name: "Loaded/Get", CacheKey: loadedCacheKey, Run: loadedBenchGet},
		Bench{Name: "Loaded/GetMiss", Backends: []string{"LSM", "Pebble", "Bolt"}, CacheKey: loadedCacheKey, Run: loadedBenchGetMiss},
		Bench{Name: "Loaded/Scan", CacheKey: loadedCacheKey, Run: loadedBenchScan},
		Bench{Name: "Loaded/ReverseScan", CacheKey: loadedCacheKey, Run: loadedBenchReverseScan},
	)
	for _, n := range []int{1, 2, 4, 8, 16} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("Loaded/ConcurrentReads/%dr", n),
			Backends: []string{"LSM", "Pebble"},
			CacheKey: loadedCacheKey,
			Run:      loadedConcurrentReaders(n),
		})
	}
	for _, n := range []int{2, 4, 8, 16} {
		out = append(out, Bench{
			Name:     fmt.Sprintf("Loaded/ConcurrentReadWrites/%dg", n),
			Backends: []string{"LSM", "Pebble"},
			CacheKey: loadedCacheKey,
			Run:      loadedConcurrentReadWrites(n),
		})
	}

	return out
}

// =============================================================================
// Loaded benchmarks: same shape as the small read benches, but against a
// pre-populated DB. The cache is keyed so all four loaded variants share
// one populated copy per backend.
// =============================================================================

const (
	loadedSize     = 500_000
	loadedCacheKey = "loaded-500K"
)

func loadedBenchGet(db DB, budget time.Duration) (int, error) {
	idx := genReadIndices(100_000, loadedSize, indexSeed)
	return loopFor(budget, func(i int) error {
		_, err := db.Get(uint64Key(uint64(idx[i%len(idx)])))
		return err
	})
}

func loadedBenchGetMiss(db DB, budget time.Duration) (int, error) {
	// Populated key range is [0, loadedSize). Query keys above it.
	return loopFor(budget, func(i int) error {
		_, _ = db.Get(uint64Key(uint64(loadedSize + i)))
		return nil
	})
}

func loadedBenchScan(db DB, budget time.Duration) (int, error) {
	starts := genReadIndices(10_000, loadedSize-scanBatch, indexSeed)
	return loopFor(budget, func(i int) error {
		s := starts[i%len(starts)]
		it := db.Scan(uint64Key(uint64(s)), uint64Key(uint64(s+scanBatch)))
		for it.Next() {
		}
		return it.Close()
	})
}

func loadedBenchReverseScan(db DB, budget time.Duration) (int, error) {
	starts := genReadIndices(10_000, loadedSize-scanBatch, indexSeed)
	return loopFor(budget, func(i int) error {
		s := starts[i%len(starts)]
		it := db.ReverseScan(uint64Key(uint64(s)), uint64Key(uint64(s+scanBatch)))
		for it.Next() {
		}
		return it.Close()
	})
}

// =============================================================================
// Driver.
// =============================================================================

type opener func(dir string) (DB, error)

var openers = map[string]opener{
	"LSM":    openLSM,
	"Pebble": openPebble,
	"SQLite": openSQLite,
	"Bolt":   openBolt,
}

// backendOrder pins the column order in the rendered table.
var backendOrder = []string{"LSM", "Pebble", "SQLite", "Bolt"}

type Result struct {
	Bench      string
	Backend    string
	Ops        int
	Wall       time.Duration
	PeakHeap   uint64
	LiveHeap   uint64
	AllocBytes uint64
	Allocs     uint64
	Err        error
}

// runOne executes one bench × backend combination. Opens a fresh DB in
// a tmpdir, runs the bench for `budget` while polling HeapInuse, then
// closes and tears down. Returns metrics.
func runOne(b Bench, backend string, budget time.Duration) Result {
	res := Result{Bench: b.Name, Backend: backend}

	// Cached benches reuse a populated dir on disk; non-cached benches
	// get a fresh tempdir we delete on the way out.
	var dir string
	if b.CacheKey != "" {
		dir = cacheDir(b.CacheKey, backend)
		if !isPopulated(dir) {
			res.Err = fmt.Errorf("cache missing for %s/%s — run: go run ./benchmark populate -key %s",
				b.Name, backend, b.CacheKey)
			return res
		}
	} else {
		d, err := os.MkdirTemp("", "bench-*")
		if err != nil {
			res.Err = err
			return res
		}
		dir = d
		defer os.RemoveAll(d)
	}

	open := openers[backend]
	db, err := open(dir)
	if err != nil {
		res.Err = err
		return res
	}

	// Steady the heap before snapshotting.
	runtime.GC()
	var msStart runtime.MemStats
	runtime.ReadMemStats(&msStart)

	// Poll HeapInuse during the run; record the max.
	var peak atomic.Uint64
	stopPoll := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-stopPoll:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peak.Load() {
					peak.Store(ms.HeapInuse)
				}
			}
		}
	}()

	start := time.Now()
	ops, runErr := b.Run(db, budget)
	res.Wall = time.Since(start)
	close(stopPoll)
	<-pollDone

	runtime.GC()
	var msEnd runtime.MemStats
	runtime.ReadMemStats(&msEnd)

	db.Close()

	res.Ops = ops
	res.PeakHeap = peak.Load()
	res.LiveHeap = msEnd.HeapInuse
	res.AllocBytes = msEnd.TotalAlloc - msStart.TotalAlloc
	res.Allocs = msEnd.Mallocs - msStart.Mallocs
	res.Err = runErr
	return res
}

// =============================================================================
// Rendering.
// =============================================================================

func fmtNs(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2fus", ns/1e3)
	}
	return fmt.Sprintf("%.0fns", ns)
}

func fmtBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2fGB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.0fMB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0fKB", float64(b)/float64(1<<10))
	}
	return fmt.Sprintf("%dB", b)
}

// nsPerOp returns wall time per op, or 0 if no ops completed.
func nsPerOp(r Result) float64 {
	if r.Err != nil || r.Ops == 0 {
		return 0
	}
	return float64(r.Wall.Nanoseconds()) / float64(r.Ops)
}

// formatCell builds the per-backend cell. Format:
//
//	`<time>  <ratio>x  <peak>/<live>`  (non-baseline, baseline measured)
//	`<time>  <peak>/<live>`            (baseline)
//	`-`                                 (no result)
//
// ratio is baseline_ns / this_ns, so ratio>1 means "this backend is faster
// than baseline". `peak` is the max HeapInuse seen during the run (catches
// GC-pending memory + live); `live` is HeapInuse after a forced GC at end
// of run, approximating actually-resident bytes. A large peak/live ratio
// means the engine is allocation-heavy but the steady-state footprint is
// small.
func formatCell(r Result, base Result, isBaseline bool) string {
	if r.Err != nil || r.Ops == 0 {
		return "-"
	}
	this := nsPerOp(r)
	heap := fmt.Sprintf("%s/%s", fmtBytes(r.PeakHeap), fmtBytes(r.LiveHeap))
	if isBaseline {
		return fmt.Sprintf("%s  %s", fmtNs(this), heap)
	}
	baseNs := nsPerOp(base)
	if baseNs <= 0 || this <= 0 {
		return fmt.Sprintf("%s  %s", fmtNs(this), heap)
	}
	return fmt.Sprintf("%s  %.2fx  %s", fmtNs(this), baseNs/this, heap)
}

// printRow renders one bench × backends row. The winner column (lowest
// ns/op) is bolded when stdout is a TTY.
func printRow(name string, results map[string]Result, baseline string, opW, colW int, bold bool) {
	fmt.Printf("%-*s", opW, name)

	base := results[baseline]

	// Identify winner by ns/op.
	winnerVal := -1.0
	winner := ""
	for _, b := range backendOrder {
		r, ok := results[b]
		if !ok {
			continue
		}
		v := nsPerOp(r)
		if v == 0 {
			continue
		}
		if winnerVal < 0 || v < winnerVal {
			winnerVal = v
			winner = b
		}
	}

	for _, b := range backendOrder {
		r, present := results[b]
		var cell string
		if !present {
			cell = "-"
		} else {
			cell = formatCell(r, base, b == baseline)
		}
		pad := colW - len(cell)
		if pad < 0 {
			pad = 0
		}
		fmt.Printf("%*s", pad, "")
		if bold && b == winner && winner != "" {
			fmt.Printf("\033[1m%s\033[0m", cell)
		} else {
			fmt.Print(cell)
		}
	}
	fmt.Println()
}

// =============================================================================
// main + subcommands.
// =============================================================================

func main() {
	// First non-flag argument is the subcommand. Default is "bench".
	args := os.Args[1:]
	cmd := "bench"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd = args[0]
		args = args[1:]
	}

	switch cmd {
	case "bench":
		runBench(args)
	case "populate":
		runPopulate(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q (want: bench | populate)\n", cmd)
		os.Exit(2)
	}
}

// runPopulate fills the on-disk cache with a ~500MB seeded dataset for
// each backend. Idempotent — a stamp file marks each cache dir as done,
// so re-running skips already-populated backends. The bench tool's
// Loaded/* benches will refuse to run until this has happened.
func runPopulate(args []string) {
	fs := flag.NewFlagSet("populate", flag.ExitOnError)
	size := fs.Int("size", loadedSize, "number of entries to populate per backend")
	key := fs.String("key", loadedCacheKey, "cache key (subdir name under the cache root)")
	backendsFlag := fs.String("backends", "LSM,Pebble,SQLite,Bolt", "comma-separated list of backends to populate")
	force := fs.Bool("force", false, "re-populate even if cache is already stamped")
	fs.Parse(args)

	wanted := map[string]bool{}
	for _, b := range splitCSV(*backendsFlag) {
		if _, ok := openers[b]; !ok {
			fmt.Fprintf(os.Stderr, "unknown backend %q\n", b)
			os.Exit(2)
		}
		wanted[b] = true
	}

	entries := genWorkload(*size, dataSeed)
	rawBytes := 0
	for _, e := range entries {
		rawBytes += len(e.value)
	}
	fmt.Printf("populating cache key=%q size=%d (~%s raw)\n", *key, *size, fmtBytes(uint64(rawBytes)))
	fmt.Printf("cache root: %s\n\n", cacheRoot())

	for _, backend := range backendOrder {
		if !wanted[backend] {
			continue
		}
		dir := cacheDir(*key, backend)
		if isPopulated(dir) && !*force {
			fmt.Printf("[%s] already populated, skipping (use -force to redo)\n", backend)
			continue
		}
		if *force {
			os.RemoveAll(dir)
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] mkdir: %v\n", backend, err)
			continue
		}

		fmt.Printf("[%s] populating into %s ...\n", backend, dir)
		t0 := time.Now()
		db, err := openers[backend](dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] open: %v\n", backend, err)
			continue
		}
		if err := populate(db, entries); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] populate: %v\n", backend, err)
			db.Close()
			continue
		}
		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] close: %v\n", backend, err)
			continue
		}
		if err := markPopulated(dir); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] stamp: %v\n", backend, err)
			continue
		}
		fmt.Printf("[%s] done in %s\n", backend, time.Since(t0).Round(time.Millisecond))
	}
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	filter := fs.String("bench", "", "regex filter on bench names (default: run all)")
	backendFilter := fs.String("backends", "", "comma-separated backends to run (default: all)")
	budget := fs.Duration("budget", 1*time.Second, "wall budget per bench × backend")
	baseline := fs.String("baseline", "Pebble", "backend used as ratio baseline")
	bold := fs.Bool("bold", true, "highlight winner in bold (auto-disabled when stdout is not a tty)")
	fs.Parse(args)

	// Optional per-backend filter — useful when measuring per-backend RSS,
	// since multiple backends in one process pollute each other's metrics.
	allowedBackends := backendOrder
	if *backendFilter != "" {
		allowedBackends = splitCSV(*backendFilter)
	}

	var pattern *regexp.Regexp
	if *filter != "" {
		p, err := regexp.Compile(*filter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bad -bench regex: %v\n", err)
			os.Exit(2)
		}
		pattern = p
	}

	bs := benches()
	if pattern != nil {
		filtered := bs[:0]
		for _, b := range bs {
			if pattern.MatchString(b.Name) {
				filtered = append(filtered, b)
			}
		}
		bs = filtered
	}
	if len(bs) == 0 {
		fmt.Println("no benches matched filter")
		return
	}

	// Stable order for column width calculation.
	sort.SliceStable(bs, func(i, j int) bool { return bs[i].Name < bs[j].Name })

	opW := len("operation") + 2
	for _, b := range bs {
		if len(b.Name)+2 > opW {
			opW = len(b.Name) + 2
		}
	}
	const colW = 28

	useBold := *bold && isTTY()

	// Header. Each cell shows time / ratio / peak heap together.
	header := fmt.Sprintf("budget=%s baseline=%s  (cell: time  ratio  peak/live heap)\n\n", *budget, *baseline)
	fmt.Print(header)
	fmt.Printf("%-*s", opW, "operation")
	for _, b := range backendOrder {
		label := b
		if b == *baseline {
			label = b + " (base)"
		}
		fmt.Printf("%*s", colW, label)
	}
	fmt.Println()
	for i := 0; i < opW+colW*len(backendOrder); i++ {
		fmt.Print("-")
	}
	fmt.Println()

	allowedSet := make(map[string]bool, len(allowedBackends))
	for _, b := range allowedBackends {
		allowedSet[b] = true
	}

	for _, bench := range bs {
		applicable := bench.Backends
		if len(applicable) == 0 {
			applicable = backendOrder
		}
		results := make(map[string]Result)
		for _, backend := range applicable {
			if !allowedSet[backend] {
				continue
			}
			r := runOne(bench, backend, *budget)
			results[backend] = r
			if r.Err != nil {
				fmt.Fprintf(os.Stderr, "  %s/%s: %v\n", bench.Name, backend, r.Err)
			}
		}
		printRow(bench.Name, results, *baseline, opW, colW, useBold)
	}
}

// isTTY returns true if stdout is a terminal. Used to gate ANSI escapes.
func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
