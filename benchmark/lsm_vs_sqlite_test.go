package benchmark

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	_ "github.com/mattn/go-sqlite3"
	bolt "go.etcd.io/bbolt"

	"github.com/germtb/protodb"
)

func now() time.Time                  { return time.Now() }
func since(t time.Time) time.Duration { return time.Since(t) }

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

var boltBucket = []byte("kv")

func initBolt(b *testing.B) *bolt.DB {
	b.Helper()
	db, err := bolt.Open(filepath.Join(b.TempDir(), "bolt.db"), 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	}); err != nil {
		b.Fatal(err)
	}
	return db
}

// initKVSQLite opens a SQLite DB with a minimal KV schema matching the LSM's
// key-value model: (key INTEGER PRIMARY KEY, value BLOB).
func initKVSQLite(b *testing.B, synchronous string) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(b.TempDir(), "kv.db"))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA synchronous=` + synchronous); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE kv (key INTEGER PRIMARY KEY, value BLOB) WITHOUT ROWID`); err != nil {
		b.Fatal(err)
	}
	return db
}

func uint64Key(key uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, key)
	return buf
}

// keyPool pre-allocates keys to avoid measuring allocation overhead in benchmarks.
var keyPool [][]byte

func init() {
	const poolSize = 1_000_000
	keyPool = make([][]byte, poolSize)
	for idx := range keyPool {
		keyPool[idx] = uint64Key(uint64(idx))
	}
}

func poolKey(idx int) []byte {
	return keyPool[idx%len(keyPool)]
}

// pebbleOptionsMatched returns a Pebble config tuned to match our engine:
// - 64MB memtable (vs Pebble's 4MB default) so flush cadence is comparable.
// - BytesPerSync=0 to disable the periodic sync_file_range hint during SST
//   writes. On Linux the default 512KB hint isn't a durability fsync — it
//   only tells the kernel to start write-back — but we disable it so both
//   engines do zero background I/O pressure during the write path.
// - WALBytesPerSync=0 to disable background WAL sync (already the default).
func pebbleOptionsMatched() *pebble.Options {
	return &pebble.Options{
		MemTableSize:    64 * 1024 * 1024,
		BytesPerSync:    0,
		WALBytesPerSync: 0,
	}
}

func initPebble(b *testing.B) *pebble.DB {
	b.Helper()
	db, err := pebble.Open(filepath.Join(b.TempDir(), "pebble"), pebbleOptionsMatched())
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func initPebbleT(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(filepath.Join(t.TempDir(), "pebble"), pebbleOptionsMatched())
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func initBoltT(t *testing.T) *bolt.DB {
	t.Helper()
	db, err := bolt.Open(filepath.Join(t.TempDir(), "bolt.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func initLSM(b *testing.B) *protodb.Engine {
	b.Helper()
	engine, err := protodb.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	return engine
}

// BenchmarkLSMvsSQLite compares the LSM engine against raw SQLite for
// equivalent write workloads. Both engines use a WAL — SQLite's built-in
// WAL journal mode, and the LSM's append-only WAL file. Neither side
// fsyncs per write, so both have the same durability guarantee: survives
// process crash, not power loss.
//
// SQLite NORMAL: WAL fsynced lazily at checkpoints (default production config).
// LSM: WAL append per Put, no fsync (same durability as SQLite NORMAL).
func BenchmarkLSMvsSQLite(b *testing.B) {
	val := mustMarshal(b, makeItem(0))

	for _, batchSize := range []int{1, 10, 100, 1000} {
		// --- SQLite NORMAL (WAL, no fsync per commit) ---
		b.Run(fmt.Sprintf("Write%d/SQLite", batchSize), func(b *testing.B) {
			db := initKVSQLite(b, "NORMAL")
			defer db.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				stmt, err := tx.Prepare(`INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
				if err != nil {
					b.Fatal(err)
				}
				for idx := 0; idx < batchSize; idx++ {
					if _, err := stmt.Exec(int64(iter*batchSize+idx), val); err != nil {
						b.Fatal(err)
					}
				}
				stmt.Close()
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})

		// --- LSM: N Puts in a transaction (one WAL syscall per batch) ---
		b.Run(fmt.Sprintf("Write%d/LSM", batchSize), func(b *testing.B) {
			engine := initLSM(b)
			defer engine.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				tx := engine.Transaction()
				for idx := 0; idx < batchSize; idx++ {
					tx.Put(poolKey(iter*batchSize+idx), val)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})

		// --- Pebble: N Puts in a batch ---
		b.Run(fmt.Sprintf("Write%d/Pebble", batchSize), func(b *testing.B) {
			db := initPebble(b)
			defer db.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				batch := db.NewBatch()
				for idx := 0; idx < batchSize; idx++ {
					batch.Set(poolKey(iter*batchSize+idx), val, nil)
				}
				if err := batch.Commit(pebble.NoSync); err != nil {
					b.Fatal(err)
				}
			}
		})

	}

	// --- Get: single key lookup after populating + flushing ---

	const populateSize = 10000

	b.Run("Get/SQLite", func(b *testing.B) {
		db := initKVSQLite(b, "NORMAL")
		defer db.Close()
		tx, _ := db.Begin()
		stmt, _ := tx.Prepare(`INSERT INTO kv(key, value) VALUES(?, ?)`)
		for idx := 0; idx < populateSize; idx++ {
			stmt.Exec(int64(idx), val)
		}
		stmt.Close()
		tx.Commit()

		lookup, _ := db.Prepare(`SELECT value FROM kv WHERE key = ?`)
		defer lookup.Close()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			var out []byte
			if err := lookup.QueryRow(int64(iter % populateSize)).Scan(&out); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get/LSM", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for idx := 0; idx < populateSize; idx++ {
			engine.Put(uint64Key(uint64(idx)), val)
		}
		engine.Flush()
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			if _, err := engine.Get(poolKey(iter % populateSize)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get/Pebble", func(b *testing.B) {
		db := initPebble(b)
		defer db.Close()
		for idx := 0; idx < populateSize; idx++ {
			db.Set(uint64Key(uint64(idx)), val, pebble.NoSync)
		}
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			v, closer, err := db.Get(poolKey(iter % populateSize))
			if err != nil {
				b.Fatal(err)
			}
			_ = v
			closer.Close()
		}
	})

	b.Run("Get/Bolt", func(b *testing.B) {
		db := initBolt(b)
		defer db.Close()
		db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(boltBucket)
			for idx := 0; idx < populateSize; idx++ {
				bucket.Put(uint64Key(uint64(idx)), val)
			}
			return nil
		})
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			db.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(boltBucket)
				_ = bucket.Get(poolKey(iter % populateSize))
				return nil
			})
		}
	})

	// --- GetMiss: lookup keys that don't exist ---

	b.Run("GetMiss/LSM", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for idx := 0; idx < populateSize; idx++ {
			engine.Put(uint64Key(uint64(idx*2)), val) // even keys only
		}
		engine.Flush()
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			engine.Get(uint64Key(uint64(iter*2 + 1))) // odd keys: always miss
		}
	})

	b.Run("GetMiss/Pebble", func(b *testing.B) {
		db := initPebble(b)
		defer db.Close()
		for idx := 0; idx < populateSize; idx++ {
			db.Set(uint64Key(uint64(idx*2)), val, pebble.NoSync)
		}
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			db.Get(uint64Key(uint64(iter*2 + 1)))
		}
	})

	b.Run("GetMiss/Bolt", func(b *testing.B) {
		db := initBolt(b)
		defer db.Close()
		db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(boltBucket)
			for idx := 0; idx < populateSize; idx++ {
				bucket.Put(uint64Key(uint64(idx*2)), val)
			}
			return nil
		})
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			db.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(boltBucket)
				_ = bucket.Get(uint64Key(uint64(iter*2 + 1)))
				return nil
			})
		}
	})

	// --- Scan: range scan over populated data ---

	b.Run("Scan1000/SQLite", func(b *testing.B) {
		db := initKVSQLite(b, "NORMAL")
		defer db.Close()
		tx, _ := db.Begin()
		stmt, _ := tx.Prepare(`INSERT INTO kv(key, value) VALUES(?, ?)`)
		for idx := 0; idx < populateSize; idx++ {
			stmt.Exec(int64(idx), val)
		}
		stmt.Close()
		tx.Commit()

		scan, _ := db.Prepare(`SELECT key, value FROM kv WHERE key >= ? AND key < ?`)
		defer scan.Close()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			rows, err := scan.Query(int64(0), int64(1000))
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for rows.Next() {
				var key int64
				var value []byte
				rows.Scan(&key, &value)
				count++
			}
			rows.Close()
			if count != 1000 {
				b.Fatalf("expected 1000 rows, got %d", count)
			}
		}
	})

	b.Run("Scan1000/LSM", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for idx := 0; idx < populateSize; idx++ {
			engine.Put(uint64Key(uint64(idx)), val)
		}
		engine.Flush()
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			count := 0
			scanner := engine.Scan(poolKey(0), poolKey(1000))
			for scanner.Next() {
				count++
			}
			if count != 1000 {
				b.Fatalf("expected 1000 entries, got %d", count)
			}
		}
	})

	b.Run("Scan1000/Pebble", func(b *testing.B) {
		db := initPebble(b)
		defer db.Close()
		for idx := 0; idx < populateSize; idx++ {
			db.Set(uint64Key(uint64(idx)), val, pebble.NoSync)
		}
		db.Flush()
		lo := uint64Key(0)
		hi := uint64Key(1000)
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			it, _ := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
			count := 0
			for it.First(); it.Valid(); it.Next() {
				_ = it.Value()
				count++
			}
			it.Close()
			if count != 1000 {
				b.Fatalf("expected 1000 entries, got %d", count)
			}
		}
	})

	b.Run("Scan1000/Bolt", func(b *testing.B) {
		db := initBolt(b)
		defer db.Close()
		db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(boltBucket)
			for idx := 0; idx < populateSize; idx++ {
				bucket.Put(uint64Key(uint64(idx)), val)
			}
			return nil
		})
		hi := uint64Key(1000)
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			db.View(func(tx *bolt.Tx) error {
				cursor := tx.Bucket(boltBucket).Cursor()
				count := 0
				for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
					if bytes.Compare(k, hi) >= 0 {
						break
					}
					count++
				}
				if count != 1000 {
					b.Fatalf("expected 1000 entries, got %d", count)
				}
				return nil
			})
		}
	})

	// --- ReverseScan: same range, descending order ---

	b.Run("ReverseScan1000/SQLite", func(b *testing.B) {
		db := initKVSQLite(b, "NORMAL")
		defer db.Close()
		tx, _ := db.Begin()
		stmt, _ := tx.Prepare(`INSERT INTO kv(key, value) VALUES(?, ?)`)
		for idx := 0; idx < populateSize; idx++ {
			stmt.Exec(int64(idx), val)
		}
		stmt.Close()
		tx.Commit()

		scan, _ := db.Prepare(`SELECT key, value FROM kv WHERE key >= ? AND key < ? ORDER BY key DESC`)
		defer scan.Close()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			rows, err := scan.Query(int64(0), int64(1000))
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for rows.Next() {
				var key int64
				var value []byte
				rows.Scan(&key, &value)
				count++
			}
			rows.Close()
			if count != 1000 {
				b.Fatalf("expected 1000 rows, got %d", count)
			}
		}
	})

	b.Run("ReverseScan1000/LSM", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for idx := 0; idx < populateSize; idx++ {
			engine.Put(uint64Key(uint64(idx)), val)
		}
		engine.Flush()
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			count := 0
			scanner := engine.ReverseScan(poolKey(0), poolKey(1000))
			for scanner.Next() {
				count++
			}
			if count != 1000 {
				b.Fatalf("expected 1000 entries, got %d", count)
			}
		}
	})

	b.Run("ReverseScan1000/Pebble", func(b *testing.B) {
		db := initPebble(b)
		defer db.Close()
		for idx := 0; idx < populateSize; idx++ {
			db.Set(uint64Key(uint64(idx)), val, pebble.NoSync)
		}
		db.Flush()
		lo := uint64Key(0)
		hi := uint64Key(1000)
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			it, _ := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
			count := 0
			for it.Last(); it.Valid(); it.Prev() {
				_ = it.Value()
				count++
			}
			it.Close()
			if count != 1000 {
				b.Fatalf("expected 1000 entries, got %d", count)
			}
		}
	})

	b.Run("ReverseScan1000/Bolt", func(b *testing.B) {
		db := initBolt(b)
		defer db.Close()
		db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(boltBucket)
			for idx := 0; idx < populateSize; idx++ {
				bucket.Put(uint64Key(uint64(idx)), val)
			}
			return nil
		})
		lo := uint64Key(0)
		hi := uint64Key(1000)
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			db.View(func(tx *bolt.Tx) error {
				cursor := tx.Bucket(boltBucket).Cursor()
				count := 0
				// Position at the first key >= hi, then step back to the last
				// key < hi (or to Last() if hi is past the end).
				k, _ := cursor.Seek(hi)
				if k == nil {
					k, _ = cursor.Last()
				} else {
					k, _ = cursor.Prev()
				}
				for ; k != nil; k, _ = cursor.Prev() {
					if bytes.Compare(k, lo) < 0 {
						break
					}
					count++
				}
				if count != 1000 {
					b.Fatalf("expected 1000 entries, got %d", count)
				}
				return nil
			})
		}
	})
}

// loadedCacheVersion gates reuse of persistent populate dirs. Bump whenever
// an on-disk format changes (SST footer, entry layout, manifest, WAL) so
// stale caches get rebuilt instead of silently returning corrupt data.
const loadedCacheVersion = 2

// loadedCacheDir returns a stable directory for a cached, pre-populated DB
// of the given engine. The caller checks for a ".loaded" sentinel inside; if
// present, the populate step is skipped. If absent (first run or prior
// crash), the caller wipes the dir and repopulates.
//
// PROTODB_REFRESH_CACHE=1 forces a fresh populate regardless of sentinel.
func loadedCacheDir(engine string) string {
	return filepath.Join(os.TempDir(),
		fmt.Sprintf("protodb-loaded-v%d", loadedCacheVersion), engine)
}

func isPopulated(dir string) bool {
	if os.Getenv("PROTODB_REFRESH_CACHE") == "1" {
		return false
	}
	_, err := os.Stat(filepath.Join(dir, ".loaded"))
	return err == nil
}

func markPopulated(dir string) error {
	return os.WriteFile(filepath.Join(dir, ".loaded"), nil, 0644)
}

// prepareCacheDir ensures `dir` exists and is empty (stale dirs without the
// .loaded sentinel are wiped). Used before a populate.
func prepareCacheDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
}

// TestLoadedPerformance measures Get, GetMiss, and Scan after populating
// ~500 MB of data into each engine. Uses persistent cache dirs so repeat
// runs skip the ~minutes-long populate step. Bump loadedCacheVersion to
// invalidate caches after a format change.
func TestLoadedPerformance(t *testing.T) {
	val := make([]byte, 100)
	const totalEntries = 4_300_000 // ~500 MB at 120 bytes/entry
	const batchSize = 10000
	const samples = 1_000_000

	measure := func(name string, fn func(i int)) {
		start := now()
		for i := 0; i < samples; i++ {
			fn(i)
		}
		dur := since(start)
		t.Logf("%-25s  %d ns/op", name, dur.Nanoseconds()/int64(samples))
	}

	// --- LSM ---
	lsmDir := loadedCacheDir("lsm")
	if !isPopulated(lsmDir) {
		t.Log("Populating LSM (first run)...")
		prepareCacheDir(t, lsmDir)
		engine, _ := protodb.Open(lsmDir)
		// No SetPolicy — defaults give us auto-flush at 64MB and
		// auto-compact at 4 L0 SSTs. The engine handles the rest.
		for idx := 0; idx < totalEntries; idx++ {
			engine.Put(uint64Key(uint64(idx)), val)
		}
		// One explicit Flush at the end drains the final memtable remainder
		// into an SST so the reopen doesn't pay for WAL replay; Compact then
		// folds that last L0 SST into L1 so the cached state is L1-only and
		// the measurements hit the same code path every run.
		engine.Flush()
		engine.Compact()
		engine.Close()
		if err := markPopulated(lsmDir); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Log("Reusing cached LSM")
	}
	engine, err := protodb.Open(lsmDir)
	if err != nil {
		t.Fatal(err)
	}
	s := engine.Stats()
	t.Logf("LSM loaded: %d SSTs, %.1f MB", s.L1SSTs, float64(s.L1Bytes)/(1024*1024))

	measure("Get/LSM", func(i int) {
		engine.Get(poolKey(i % totalEntries))
	})
	measure("GetMiss/LSM", func(i int) {
		engine.Get(uint64Key(uint64(totalEntries + i))) // keys past the range
	})
	measure("Scan1000/LSM", func(i int) {
		scanner := engine.Scan(poolKey(0), poolKey(1000))
		for scanner.Next() {
		}
	})
	measure("ReverseScan1000/LSM", func(i int) {
		scanner := engine.ReverseScan(poolKey(0), poolKey(1000))
		for scanner.Next() {
		}
	})
	engine.Close()

	// --- Pebble ---
	pebbleDir := loadedCacheDir("pebble")
	if !isPopulated(pebbleDir) {
		t.Log("Populating Pebble (first run)...")
		prepareCacheDir(t, pebbleDir)
		pdb, err := pebble.Open(pebbleDir, pebbleOptionsMatched())
		if err != nil {
			t.Fatal(err)
		}
		batch := pdb.NewBatch()
		for idx := 0; idx < totalEntries; idx++ {
			batch.Set(uint64Key(uint64(idx)), val, nil)
			if (idx+1)%batchSize == 0 {
				batch.Commit(pebble.NoSync)
				batch = pdb.NewBatch()
			}
		}
		if batch.Count() > 0 {
			batch.Commit(pebble.NoSync)
		}
		pdb.Flush()
		pdb.Compact(uint64Key(0), uint64Key(uint64(totalEntries)), true)
		pdb.Close()
		if err := markPopulated(pebbleDir); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Log("Reusing cached Pebble")
	}
	pdb, err := pebble.Open(pebbleDir, pebbleOptionsMatched())
	if err != nil {
		t.Fatal(err)
	}

	measure("Get/Pebble", func(i int) {
		v, closer, err := pdb.Get(poolKey(i % totalEntries))
		if err != nil {
			return
		}
		_ = v
		closer.Close()
	})
	measure("GetMiss/Pebble", func(i int) {
		pdb.Get(uint64Key(uint64(totalEntries + i)))
	})
	lo := uint64Key(0)
	hi := uint64Key(1000)
	measure("Scan1000/Pebble", func(i int) {
		it, _ := pdb.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		for it.First(); it.Valid(); it.Next() {
			_ = it.Value()
		}
		it.Close()
	})
	measure("ReverseScan1000/Pebble", func(i int) {
		it, _ := pdb.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
		for it.Last(); it.Valid(); it.Prev() {
			_ = it.Value()
		}
		it.Close()
	})
	pdb.Close()

	// --- Bolt ---
	boltDir := loadedCacheDir("bolt")
	boltPath := filepath.Join(boltDir, "bolt.db")
	openBolt := func() *bolt.DB {
		db, err := bolt.Open(boltPath, 0600, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(boltBucket)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	if !isPopulated(boltDir) {
		t.Log("Populating Bolt (first run)...")
		prepareCacheDir(t, boltDir)
		bdb := openBolt()
		for start := 0; start < totalEntries; start += batchSize {
			end := start + batchSize
			if end > totalEntries {
				end = totalEntries
			}
			bdb.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(boltBucket)
				for idx := start; idx < end; idx++ {
					bucket.Put(uint64Key(uint64(idx)), val)
				}
				return nil
			})
		}
		bdb.Close()
		if err := markPopulated(boltDir); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Log("Reusing cached Bolt")
	}
	bdb := openBolt()

	measure("Get/Bolt", func(i int) {
		bdb.View(func(tx *bolt.Tx) error {
			_ = tx.Bucket(boltBucket).Get(poolKey(i % totalEntries))
			return nil
		})
	})
	measure("GetMiss/Bolt", func(i int) {
		bdb.View(func(tx *bolt.Tx) error {
			_ = tx.Bucket(boltBucket).Get(uint64Key(uint64(totalEntries + i)))
			return nil
		})
	})
	// Scan + ReverseScan over [0, 1000) on the loaded Bolt DB.
	boltLo := uint64Key(0)
	boltHi := uint64Key(1000)
	measure("Scan1000/Bolt", func(i int) {
		bdb.View(func(tx *bolt.Tx) error {
			cursor := tx.Bucket(boltBucket).Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				if bytes.Compare(k, boltHi) >= 0 {
					break
				}
			}
			return nil
		})
	})
	measure("ReverseScan1000/Bolt", func(i int) {
		bdb.View(func(tx *bolt.Tx) error {
			cursor := tx.Bucket(boltBucket).Cursor()
			// Position at the first key >= hi, step back to land on < hi.
			k, _ := cursor.Seek(boltHi)
			if k == nil {
				k, _ = cursor.Last()
			} else {
				k, _ = cursor.Prev()
			}
			for ; k != nil; k, _ = cursor.Prev() {
				if bytes.Compare(k, boltLo) < 0 {
					break
				}
			}
			return nil
		})
	})
	bdb.Close()
}

// BenchmarkSSTScaling measures how Get and Scan degrade as SSTs accumulate
// without compaction. Each SST contains 100 entries with distinct keys.
func BenchmarkSSTScaling(b *testing.B) {
	val := mustMarshal(b, makeItem(0))

	for _, sstCount := range []int{1, 5, 10, 20, 50} {
		// --- Get with N SSTs ---
		b.Run(fmt.Sprintf("Get/SSTs_%d", sstCount), func(b *testing.B) {
			engine := initLSM(b)
			defer engine.Close()

			// Write 100 entries per SST, flush each batch
			for sst := 0; sst < sstCount; sst++ {
				for idx := 0; idx < 100; idx++ {
					engine.Put(uint64Key(uint64(sst*100+idx)), val)
				}
				engine.Flush()
			}

			totalKeys := uint64(sstCount * 100)
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				_, err := engine.Get(poolKey(int(uint64(iter) % totalKeys)))
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// --- Scan with N SSTs ---
		b.Run(fmt.Sprintf("Scan1000/SSTs_%d", sstCount), func(b *testing.B) {
			engine := initLSM(b)
			defer engine.Close()

			keysPerSST := 1000 / sstCount
			if keysPerSST < 1 {
				keysPerSST = 1
			}
			for sst := 0; sst < sstCount; sst++ {
				for idx := 0; idx < keysPerSST; idx++ {
					engine.Put(uint64Key(uint64(sst*keysPerSST+idx)), val)
				}
				engine.Flush()
			}

			lo := poolKey(0)
			hi := poolKey(sstCount * keysPerSST)
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				count := 0
				scanner := engine.Scan(lo, hi)
				for scanner.Next() {
					count++
				}
			}
		})
	}

	// --- Get after compaction (baseline) ---
	b.Run("Get/Compacted", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()

		for sst := 0; sst < 20; sst++ {
			for idx := 0; idx < 100; idx++ {
				engine.Put(uint64Key(uint64(sst*100+idx)), val)
			}
			engine.Flush()
		}
		engine.Compact()

		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			_, err := engine.Get(poolKey(iter % 2000))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// --- Scan after compaction (baseline) ---
	b.Run("Scan1000/Compacted", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()

		for sst := 0; sst < 20; sst++ {
			for idx := 0; idx < 50; idx++ {
				engine.Put(uint64Key(uint64(sst*50+idx)), val)
			}
			engine.Flush()
		}
		engine.Compact()

		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			count := 0
			scanner := engine.Scan(poolKey(0), poolKey(1000))
			for scanner.Next() {
				count++
			}
		}
	})
}

// BenchmarkL0vsL1 measures Get and Scan performance with data in L0
// (unsorted, linear scan) vs L1 (sorted, binary search on SSTs).
func BenchmarkL0vsL1(b *testing.B) {
	val := mustMarshal(b, makeItem(0))
	const sstCount = 50
	const keysPerSST = 10000
	const totalKeys = sstCount * keysPerSST

	b.Run("Get/L0", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for sst := 0; sst < sstCount; sst++ {
			for idx := 0; idx < keysPerSST; idx++ {
				engine.Put(uint64Key(uint64(sst*keysPerSST+idx)), val)
			}
			engine.Flush()
		}
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			engine.Get(poolKey(int(uint64(iter) % totalKeys)))
		}
	})

	b.Run("Get/L1", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for sst := 0; sst < sstCount; sst++ {
			for idx := 0; idx < keysPerSST; idx++ {
				engine.Put(uint64Key(uint64(sst*keysPerSST+idx)), val)
			}
			engine.Flush()
		}
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			engine.Get(poolKey(int(uint64(iter) % totalKeys)))
		}
	})

	b.Run("Scan1000/L0", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for sst := 0; sst < sstCount; sst++ {
			for idx := 0; idx < keysPerSST; idx++ {
				engine.Put(uint64Key(uint64(sst*keysPerSST+idx)), val)
			}
			engine.Flush()
		}
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			count := 0
			scanner := engine.Scan(poolKey(0), poolKey(1000))
			for scanner.Next() {
				count++
			}
		}
	})

	b.Run("Scan1000/L1", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for sst := 0; sst < sstCount; sst++ {
			for idx := 0; idx < keysPerSST; idx++ {
				engine.Put(uint64Key(uint64(sst*keysPerSST+idx)), val)
			}
			engine.Flush()
		}
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			count := 0
			scanner := engine.Scan(poolKey(0), poolKey(1000))
			for scanner.Next() {
				count++
			}
		}
	})
}

// BenchmarkCompaction measures the time to compact N entries that were
// written across multiple flushes. Each iteration creates a fresh DB,
// populates it, and times only the compaction.
func BenchmarkCompaction(b *testing.B) {
	// Pool of random-sized random-content buffers so snappy can't compress
	// everything to nothing and trivialize the benchmark.
	const poolSize = 64
	const minSize, maxSize = 100, 2000
	pool := make([][]byte, poolSize)
	for i := range pool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		pool[i] = make([]byte, sz)
		rand.Read(pool[i])
	}
	nextVal := func(i int) []byte { return pool[i%poolSize] }

	for _, entryCount := range []int{10, 100, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("%d/LSM", entryCount), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				engine := initLSM(b)

				batchSize := 1000
				for idx := 0; idx < entryCount; idx++ {
					engine.Put(uint64Key(uint64(idx)), nextVal(idx))
					if (idx+1)%batchSize == 0 {
						engine.Flush()
					}
				}
				if entryCount%batchSize != 0 {
					engine.Flush()
				}

				b.StartTimer()
				engine.Compact()
				b.StopTimer()
				engine.Close()
			}
		})

		b.Run(fmt.Sprintf("%d/Pebble", entryCount), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				db := initPebble(b)

				batch := db.NewBatch()
				for idx := 0; idx < entryCount; idx++ {
					batch.Set(uint64Key(uint64(idx)), nextVal(idx), nil)
					if (idx+1)%1000 == 0 {
						batch.Commit(pebble.NoSync)
						db.Flush()
						batch = db.NewBatch()
					}
				}
				if batch.Count() > 0 {
					batch.Commit(pebble.NoSync)
					db.Flush()
				}

				b.StartTimer()
				db.Compact(uint64Key(0), uint64Key(uint64(entryCount)), true)
				b.StopTimer()
				db.Close()
			}
		})
	}
}

// BenchmarkConcurrentWrites runs `totalOps` writes split across N goroutines.
// Exposes Pebble's concurrent commit pipeline (batches share the WAL write
// lane via a lock-free pending queue) vs our single flushMutex serialization.
// Each goroutine writes distinct, non-overlapping key ranges so there's no
// key contention — only engine-level commit contention.
func BenchmarkConcurrentWrites(b *testing.B) {
	const totalOps = 500_000
	const poolSize = 64
	const minSize, maxSize = 100, 2000
	pool := make([][]byte, poolSize)
	for i := range pool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		pool[i] = make([]byte, sz)
		rand.Read(pool[i])
	}
	nextVal := func(i int) []byte { return pool[i%poolSize] }

	for _, writers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("%dw/LSM", writers), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				engine := initLSM(b)
				opsPerWriter := totalOps / writers
				var wg sync.WaitGroup
				b.StartTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < opsPerWriter; i++ {
							idx := base + i
							engine.Put(uint64Key(uint64(idx)), nextVal(idx))
						}
					}(w * opsPerWriter)
				}
				wg.Wait()
				b.StopTimer()
				engine.Close()
			}
		})

		b.Run(fmt.Sprintf("%dw/Pebble", writers), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				db := initPebble(b)
				opsPerWriter := totalOps / writers
				var wg sync.WaitGroup
				b.StartTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < opsPerWriter; i++ {
							idx := base + i
							db.Set(uint64Key(uint64(idx)), nextVal(idx), pebble.NoSync)
						}
					}(w * opsPerWriter)
				}
				wg.Wait()
				b.StopTimer()
				db.Close()
			}
		})
	}
}

// initLSMSync returns an Engine with per-commit fsync enabled. Matches the
// durability contract of pebble.WriteOptions{Sync: true}.
func initLSMSync(b *testing.B) *protodb.Engine {
	b.Helper()
	engine, err := protodb.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	engine.SetPolicy(&protodb.Policy{
		SoftCompactionThreshold: 4,
		HardCompactionThreshold: 16,
		FlushThreshold:          1024 * 1024 * 64,
		Sync:                    true,
	})
	return engine
}

// BenchmarkConcurrentWritesSync mirrors BenchmarkConcurrentWrites but with
// per-commit fsync on both sides. This is the durability contract most
// production databases need — CockroachDB's Raft log uses Sync:true to
// acknowledge writes. Pebble's commit pipeline is designed exactly for this
// case: one fsync amortized across many concurrent batches.
func BenchmarkConcurrentWritesSync(b *testing.B) {
	const totalOps = 1_000 // fsync is ~1-5ms each — keep small
	const poolSize = 64
	const minSize, maxSize = 100, 2000
	pool := make([][]byte, poolSize)
	for i := range pool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		pool[i] = make([]byte, sz)
		rand.Read(pool[i])
	}
	nextVal := func(i int) []byte { return pool[i%poolSize] }

	syncOpts := &pebble.WriteOptions{Sync: true}

	for _, writers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("%dw/LSM", writers), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				engine := initLSMSync(b)
				opsPerWriter := totalOps / writers
				var wg sync.WaitGroup
				b.StartTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < opsPerWriter; i++ {
							idx := base + i
							engine.Put(uint64Key(uint64(idx)), nextVal(idx))
						}
					}(w * opsPerWriter)
				}
				wg.Wait()
				b.StopTimer()
				engine.Close()
			}
		})

		b.Run(fmt.Sprintf("%dw/Pebble", writers), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				db := initPebble(b)
				opsPerWriter := totalOps / writers
				var wg sync.WaitGroup
				b.StartTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < opsPerWriter; i++ {
							idx := base + i
							db.Set(uint64Key(uint64(idx)), nextVal(idx), syncOpts)
						}
					}(w * opsPerWriter)
				}
				wg.Wait()
				b.StopTimer()
				db.Close()
			}
		})
	}
}

// BenchmarkConcurrentBatchedWrites runs the same total ops as
// BenchmarkConcurrentWrites, but each goroutine commits in batches of 100
// puts. Pebble's commit pipeline is designed to amortize seqnum/WAL/publish
// coordination across many ops per commit — this is where it should shine.
func BenchmarkConcurrentBatchedWrites(b *testing.B) {
	const totalOps = 500_000
	const batchSize = 100
	const poolSize = 64
	const minSize, maxSize = 100, 2000
	pool := make([][]byte, poolSize)
	for i := range pool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		pool[i] = make([]byte, sz)
		rand.Read(pool[i])
	}
	nextVal := func(i int) []byte { return pool[i%poolSize] }

	for _, writers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("%dw/LSM", writers), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				engine := initLSM(b)
				opsPerWriter := totalOps / writers
				var wg sync.WaitGroup
				b.StartTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < opsPerWriter; i += batchSize {
							tx := engine.Transaction()
							for k := 0; k < batchSize && i+k < opsPerWriter; k++ {
								idx := base + i + k
								tx.Put(uint64Key(uint64(idx)), nextVal(idx))
							}
							tx.Commit()
						}
					}(w * opsPerWriter)
				}
				wg.Wait()
				b.StopTimer()
				engine.Close()
			}
		})

		b.Run(fmt.Sprintf("%dw/Pebble", writers), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				db := initPebble(b)
				opsPerWriter := totalOps / writers
				var wg sync.WaitGroup
				b.StartTimer()
				for w := 0; w < writers; w++ {
					wg.Add(1)
					go func(base int) {
						defer wg.Done()
						for i := 0; i < opsPerWriter; i += batchSize {
							batch := db.NewBatch()
							for k := 0; k < batchSize && i+k < opsPerWriter; k++ {
								idx := base + i + k
								batch.Set(uint64Key(uint64(idx)), nextVal(idx), nil)
							}
							batch.Commit(pebble.NoSync)
							batch.Close()
						}
					}(w * opsPerWriter)
				}
				wg.Wait()
				b.StopTimer()
				db.Close()
			}
		})
	}
}

// BenchmarkSustainedWrites writes `totalOps` random-sized entries (100B–2KB)
// in a tight loop, exercising the full write path (memtable → WAL → flush →
// compaction auto-trigger). Random data defeats snappy compression so the
// memtable actually fills and flushes actually fire.
func BenchmarkSustainedWrites(b *testing.B) {
	// Pool of pre-randomized buffers of varying sizes. Each Put gets a
	// distinct slice (engines store by reference, so reusing one buffer
	// across Puts would make them all identical).
	const poolSize = 64
	const minSize, maxSize = 100, 2000
	// Sizes spread deterministically across the range; content fully random.
	pool := make([][]byte, poolSize)
	for i := range pool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		pool[i] = make([]byte, sz)
		rand.Read(pool[i])
	}
	nextVal := func(i int) []byte { return pool[i%poolSize] }

	for _, totalOps := range []int{100_000, 500_000, 2_000_000} {
		b.Run(fmt.Sprintf("%d/LSM", totalOps), func(b *testing.B) {
			var lastStats protodb.EngineStats
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				engine := initLSM(b)
				b.StartTimer()
				for idx := 0; idx < totalOps; idx++ {
					engine.Put(uint64Key(uint64(idx)), nextVal(idx))
				}
				lastStats = engine.Stats()
				engine.Close()
				b.StopTimer()
			}
			b.Logf("flushes=%d compactions=%d  L0=%d SSTs (%s)  L1=%d SSTs (%s)  total=%s",
				lastStats.FlushCount, lastStats.CompactionCount,
				lastStats.L0SSTs, formatBytes(lastStats.L0Bytes),
				lastStats.L1SSTs, formatBytes(lastStats.L1Bytes),
				formatBytes(lastStats.L0Bytes+lastStats.L1Bytes))
		})

		b.Run(fmt.Sprintf("%d/Pebble", totalOps), func(b *testing.B) {
			var lastSummary string
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				db := initPebble(b)
				b.StartTimer()
				for idx := 0; idx < totalOps; idx++ {
					db.Set(uint64Key(uint64(idx)), nextVal(idx), pebble.NoSync)
				}
				m := db.Metrics()
				var totalSSTs int
				var totalBytes uint64
				var perLevel []string
				for lvl, lm := range m.Levels {
					if lm.NumFiles == 0 {
						continue
					}
					totalSSTs += int(lm.NumFiles)
					totalBytes += uint64(lm.Size)
					perLevel = append(perLevel, fmt.Sprintf("L%d=%d/%s", lvl, lm.NumFiles, formatBytes(lm.Size)))
				}
				lastSummary = fmt.Sprintf("flushes=%d compactions=%d  %s  total=%d SSTs %s",
					m.Flush.Count, m.Compact.Count,
					strings.Join(perLevel, " "),
					totalSSTs, formatBytes(int64(totalBytes)))
				db.Close()
				b.StopTimer()
			}
			b.Logf("%s", lastSummary)
		})
	}
}

// BenchmarkMixedWorkload runs write + point-Get + Scan phases with tombstones
// sprinkled in. Reports per-phase throughput and the final engine stats so we
// can see read-amp effects of accumulated L0 SSTs.
func BenchmarkMixedWorkload(b *testing.B) {
	const poolSize = 64
	const minSize, maxSize = 100, 2000
	pool := make([][]byte, poolSize)
	for i := range pool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		pool[i] = make([]byte, sz)
		rand.Read(pool[i])
	}
	nextVal := func(i int) []byte { return pool[i%poolSize] }

	const numGets = 10_000
	const numScans = 200
	const scanSize = 500
	const deleteRate = 20 // one delete per N puts

	sizes := []int{500_000, 2_000_000}

	for _, totalOps := range sizes {
		b.Run(fmt.Sprintf("%d/LSM", totalOps), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				engine := initLSM(b)

				// ---- Write phase (with tombstones) ----
				writeStart := time.Now()
				var deletes int
				for idx := 0; idx < totalOps; idx++ {
					if idx > 1000 && idx%deleteRate == 0 {
						// Delete a recent-ish key we just wrote.
						target := uint64(idx - 1 - mathrand.IntN(100))
						engine.Delete(uint64Key(target))
						deletes++
					} else {
						engine.Put(uint64Key(uint64(idx)), nextVal(idx))
					}
				}
				writeDur := time.Since(writeStart)
				stats := engine.Stats()

				// ---- Get phase (random point lookups) ----
				getStart := time.Now()
				rng := mathrand.New(mathrand.NewPCG(42, 42))
				for i := 0; i < numGets; i++ {
					k := uint64(rng.IntN(totalOps))
					engine.Get(uint64Key(k))
				}
				getDur := time.Since(getStart)

				// ---- Scan phase (range queries) ----
				scanStart := time.Now()
				var scanned int
				rng = mathrand.New(mathrand.NewPCG(7, 7))
				for i := 0; i < numScans; i++ {
					start := uint64(rng.IntN(totalOps - scanSize))
					it := engine.Scan(uint64Key(start), uint64Key(start+scanSize))
					for it.Next() {
						scanned++
					}
				}
				scanDur := time.Since(scanStart)

				engine.Close()

				b.Logf("writes=%v (%d puts+%d dels, %d/s) | gets=%v (%d ops/s) | scans=%v (%d rows/s) | flushes=%d compactions=%d L0=%d/%s L1=%d/%s",
					writeDur, totalOps-deletes, deletes,
					int(float64(totalOps)/writeDur.Seconds()),
					getDur, int(float64(numGets)/getDur.Seconds()),
					scanDur, int(float64(scanned)/scanDur.Seconds()),
					stats.FlushCount, stats.CompactionCount,
					stats.L0SSTs, formatBytes(stats.L0Bytes),
					stats.L1SSTs, formatBytes(stats.L1Bytes))
			}
		})

		b.Run(fmt.Sprintf("%d/Pebble", totalOps), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				db := initPebble(b)

				writeStart := time.Now()
				var deletes int
				for idx := 0; idx < totalOps; idx++ {
					if idx > 1000 && idx%deleteRate == 0 {
						target := uint64(idx - 1 - mathrand.IntN(100))
						db.Delete(uint64Key(target), pebble.NoSync)
						deletes++
					} else {
						db.Set(uint64Key(uint64(idx)), nextVal(idx), pebble.NoSync)
					}
				}
				writeDur := time.Since(writeStart)
				m := db.Metrics()

				getStart := time.Now()
				rng := mathrand.New(mathrand.NewPCG(42, 42))
				for i := 0; i < numGets; i++ {
					k := uint64(rng.IntN(totalOps))
					v, closer, err := db.Get(uint64Key(k))
					if err == nil {
						_ = v
						closer.Close()
					}
				}
				getDur := time.Since(getStart)

				scanStart := time.Now()
				var scanned int
				rng = mathrand.New(mathrand.NewPCG(7, 7))
				for i := 0; i < numScans; i++ {
					start := uint64(rng.IntN(totalOps - scanSize))
					it, _ := db.NewIter(&pebble.IterOptions{
						LowerBound: uint64Key(start),
						UpperBound: uint64Key(start + scanSize),
					})
					for it.First(); it.Valid(); it.Next() {
						scanned++
					}
					it.Close()
				}
				scanDur := time.Since(scanStart)

				db.Close()

				var totalSSTs int
				var totalBytes uint64
				var perLevel []string
				for lvl, lm := range m.Levels {
					if lm.NumFiles == 0 {
						continue
					}
					totalSSTs += int(lm.NumFiles)
					totalBytes += uint64(lm.Size)
					perLevel = append(perLevel, fmt.Sprintf("L%d=%d/%s", lvl, lm.NumFiles, formatBytes(lm.Size)))
				}
				b.Logf("writes=%v (%d puts+%d dels, %d/s) | gets=%v (%d ops/s) | scans=%v (%d rows/s) | flushes=%d compactions=%d %s total=%d/%s",
					writeDur, totalOps-deletes, deletes,
					int(float64(totalOps)/writeDur.Seconds()),
					getDur, int(float64(numGets)/getDur.Seconds()),
					scanDur, int(float64(scanned)/scanDur.Seconds()),
					m.Flush.Count, m.Compact.Count,
					strings.Join(perLevel, " "),
					totalSSTs, formatBytes(int64(totalBytes)))
			}
		})
	}
}

// TestCompactionOverTime measures how compaction cost grows as L1 gets larger.
// Not a benchmark (uses t.Log) because we want to observe the progression,
// not a single averaged number.
func TestCompactionOverTime(t *testing.T) {
	dir := t.TempDir()
	engine, err := protodb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Disable auto-compaction so we control when it happens.
	engine.SetPolicy(&protodb.Policy{
		FlushThreshold:      1024 * 1024 * 64, // 64MB — won't trigger auto-flush
		SoftCompactionThreshold: 1000,             // effectively disable auto-compact
	})

	const batchSize = 10000 // entries per flush (~1.2MB per batch)
	const rounds = 80       // enough to cross the 64MB SST split once or twice

	t.Logf("%-8s  %-12s  %-12s  %-10s  %-10s", "Round", "L1 SSTs", "L1 Size", "Compact ms", "Get ns")

	for round := 0; round < rounds; round++ {
		// Write a batch of entries with keys spread across the full range.
		// Use round*batchSize offset so data accumulates over time.
		for i := 0; i < batchSize; i++ {
			// Fresh buffer per Put — engine stores the slice by reference, so
			// reusing one `val` across the batch would make snappy compress L1
			// to nothing.
			val := make([]byte, 100)
			rand.Read(val)
			k := uint64(round*batchSize + i)
			engine.Put(uint64Key(k), val)
		}
		engine.Flush()

		// Measure compaction time
		compactStart := now()
		engine.Compact()
		compactDur := since(compactStart)

		// Measure Get latency (sample 100 keys)
		getStart := now()
		for i := 0; i < 100; i++ {
			k := uint64(round*batchSize + i)
			engine.Get(uint64Key(k))
		}
		getDur := since(getStart) / 100

		s := engine.Stats()

		if round < 20 || round%10 == 9 {
			t.Logf("%-8d  %-12d  %-12s  %-10.1f  %-10d",
				round+1, s.L1SSTs, formatBytes(s.L1Bytes), float64(compactDur.Microseconds())/1000.0, getDur.Nanoseconds())
		}
	}
}

// BenchmarkBlockSize measures Get and Scan performance at different block sizes.
func BenchmarkBlockSize(b *testing.B) {
	val := mustMarshal(b, makeItem(0))
	const populateSize = 10000

	// BlockSize must keep entry count ≤ maxBlockEntries (342). Sweep around 4KB.
	for _, blockSize := range []int{3800, 4096} {
		b.Run(fmt.Sprintf("Flush1000/Block_%d", blockSize), func(b *testing.B) {
			protodb.BlockSize = blockSize
			defer func() { protodb.BlockSize = 4096 }()

			engine := initLSM(b)
			defer engine.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				for idx := 0; idx < 1000; idx++ {
					engine.Put(poolKey(iter*1000+idx), val)
				}
				engine.Flush()
			}
		})

		b.Run(fmt.Sprintf("Get/Block_%d", blockSize), func(b *testing.B) {
			protodb.BlockSize = blockSize
			defer func() { protodb.BlockSize = 4096 }()

			engine := initLSM(b)
			defer engine.Close()
			for idx := 0; idx < populateSize; idx++ {
				engine.Put(uint64Key(uint64(idx)), val)
			}
			engine.Flush()
			engine.Compact()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				engine.Get(poolKey(iter % populateSize))
			}
		})

		b.Run(fmt.Sprintf("Scan1000/Block_%d", blockSize), func(b *testing.B) {
			protodb.BlockSize = blockSize
			defer func() { protodb.BlockSize = 4096 }()

			engine := initLSM(b)
			defer engine.Close()
			for idx := 0; idx < populateSize; idx++ {
				engine.Put(uint64Key(uint64(idx)), val)
			}
			engine.Flush()
			engine.Compact()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				count := 0
				scanner := engine.Scan(poolKey(0), poolKey(1000))
				for scanner.Next() {
					count++
				}
			}
		})
	}
}

// BenchmarkPartitionedSST measures Get, Scan, and Compact when SSTs are
// partitioned due to large values (100KB each). With 200 entries at 100KB
// that's ~20MB total, exceeding the default 16MB SSTSize.
func BenchmarkPartitionedSST(b *testing.B) {
	bigVal := make([]byte, 100*1024) // 100KB
	for idx := range bigVal {
		bigVal[idx] = byte(idx % 251)
	}
	const entryCount = 200

	b.Run("Get", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for idx := 0; idx < entryCount; idx++ {
			engine.Put(uint64Key(uint64(idx)), bigVal)
		}
		engine.Flush()
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			_, err := engine.Get(poolKey(iter % entryCount))
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Scan", func(b *testing.B) {
		engine := initLSM(b)
		defer engine.Close()
		for idx := 0; idx < entryCount; idx++ {
			engine.Put(uint64Key(uint64(idx)), bigVal)
		}
		engine.Flush()
		engine.Compact()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			count := 0
			scanner := engine.Scan(poolKey(0), poolKey(entryCount))
			for scanner.Next() {
				count++
			}
		}
	})

	b.Run("Compact", func(b *testing.B) {
		for iter := 0; iter < b.N; iter++ {
			b.StopTimer()
			engine := initLSM(b)
			for idx := 0; idx < entryCount; idx++ {
				engine.Put(uint64Key(uint64(idx)), bigVal)
			}
			engine.Flush()
			b.StartTimer()
			engine.Compact()
			b.StopTimer()
			engine.Close()
		}
	})

	b.Run("Get/Pebble", func(b *testing.B) {
		db := initPebble(b)
		defer db.Close()
		for idx := 0; idx < entryCount; idx++ {
			db.Set(uint64Key(uint64(idx)), bigVal, pebble.NoSync)
		}
		db.Flush()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			v, closer, err := db.Get(poolKey(iter % entryCount))
			if err != nil {
				b.Fatal(err)
			}
			_ = v
			closer.Close()
		}
	})

	b.Run("Scan/Pebble", func(b *testing.B) {
		db := initPebble(b)
		defer db.Close()
		for idx := 0; idx < entryCount; idx++ {
			db.Set(uint64Key(uint64(idx)), bigVal, pebble.NoSync)
		}
		db.Flush()
		lo := uint64Key(0)
		hi := uint64Key(uint64(entryCount))
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			it, _ := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
			count := 0
			for it.First(); it.Valid(); it.Next() {
				_ = it.Value()
				count++
			}
			it.Close()
		}
	})
}

// TestMemoryFootprint measures heap memory used by each engine after loading data.
func TestMemoryFootprint(t *testing.T) {
	val := make([]byte, 100)
	for idx := range val {
		val[idx] = byte(idx)
	}
	const entryCount = 10000

	measure := func() uint64 {
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		return mem.HeapInuse
	}

	// --- LSM ---
	before := measure()
	engine, err := protodb.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for idx := 0; idx < entryCount; idx++ {
		engine.Put(uint64Key(uint64(idx)), val)
	}
	engine.Flush()
	engine.Compact()
	lsmHeap := measure() - before
	t.Logf("LSM:    %d KB heap (%d entries)", lsmHeap/1024, entryCount)
	engine.Close()

	// --- Pebble ---
	before = measure()
	db, err := pebble.Open(filepath.Join(t.TempDir(), "pebble"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for idx := 0; idx < entryCount; idx++ {
		db.Set(uint64Key(uint64(idx)), val, pebble.NoSync)
	}
	db.Flush()
	pebbleHeap := measure() - before
	t.Logf("Pebble: %d KB heap (%d entries)", pebbleHeap/1024, entryCount)
	db.Close()
}

func TestCompressionRatio(t *testing.T) {
	val := make([]byte, 100)
	for i := range val {
		val[i] = byte(i%26 + 'a') // compressible: repeating lowercase letters
	}

	dir := t.TempDir()
	engine, _ := protodb.Open(dir)
	engine.SetPolicy(&protodb.Policy{
		FlushThreshold:      1024 * 1024 * 64,
		SoftCompactionThreshold: 1000,
	})

	const entries = 100_000
	for i := 0; i < entries; i++ {
		engine.Put(uint64Key(uint64(i)), val)
	}
	engine.Flush()
	engine.Compact()
	engine.Close()

	// Measure total size of protodb directory
	var totalSize int64
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	rawSize := int64(entries) * int64(8+100+12) // key + value + entry overhead
	t.Logf("Entries:     %d", entries)
	t.Logf("Raw data:    %s", formatBytes(rawSize))
	t.Logf("On disk:     %s", formatBytes(totalSize))
	t.Logf("Ratio:       %.2fx", float64(rawSize)/float64(totalSize))
}

func TestCompressionRatioRandom(t *testing.T) {
	rng := bytes.NewReader(nil) // placeholder
	_ = rng
	val := make([]byte, 100)
	// Fill with pseudo-random bytes (incompressible)
	for i := range val {
		val[i] = byte((i*7 + 13) % 256)
	}

	dir := t.TempDir()
	engine, _ := protodb.Open(dir)
	engine.SetPolicy(&protodb.Policy{
		FlushThreshold:      1024 * 1024 * 64,
		SoftCompactionThreshold: 1000,
	})

	const entries = 100_000
	for i := 0; i < entries; i++ {
		// Vary the value slightly per entry to prevent cross-entry compression
		val[0] = byte(i)
		val[1] = byte(i >> 8)
		val[2] = byte(i >> 16)
		engine.Put(uint64Key(uint64(i)), val)
	}
	engine.Flush()
	engine.Compact()
	engine.Close()

	var totalSize int64
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	rawSize := int64(entries) * int64(8+100+12)
	t.Logf("Entries:     %d (pseudo-random values)", entries)
	t.Logf("Raw data:    %s", formatBytes(rawSize))
	t.Logf("On disk:     %s", formatBytes(totalSize))
	t.Logf("Ratio:       %.2fx", float64(rawSize)/float64(totalSize))
}

// BenchmarkReadWriteContention measures read throughput under concurrent write
// pressure. Populates a database, then runs N reader goroutines alongside M
// writer goroutines for a fixed duration. Reports read ops/sec and write ops/sec
// separately, exposing how write-side locking affects read latency.
func BenchmarkReadWriteContention(b *testing.B) {
	const populateSize = 100_000
	const duration = 2 * time.Second

	const poolSize = 64
	const minSize, maxSize = 100, 2000
	valPool := make([][]byte, poolSize)
	for i := range valPool {
		sz := minSize + i*(maxSize-minSize)/(poolSize-1)
		valPool[i] = make([]byte, sz)
		rand.Read(valPool[i])
	}
	nextVal := func(i int) []byte { return valPool[i%poolSize] }

	type result struct {
		readOps  int64
		writeOps int64
	}

	for _, readers := range []int{1, 4, 8} {
		for _, writers := range []int{0, 1, 4} {
			tag := fmt.Sprintf("%dr_%dw", readers, writers)

			b.Run(tag+"/LSM", func(b *testing.B) {
				for iter := 0; iter < b.N; iter++ {
					b.StopTimer()
					engine := initLSM(b)
					for idx := 0; idx < populateSize; idx++ {
						engine.Put(uint64Key(uint64(idx)), nextVal(idx))
					}
					engine.Flush()
					engine.Compact()

					var readOps, writeOps int64
					var wg sync.WaitGroup
					stop := make(chan struct{})

					// Readers: point Gets on existing keys
					for reader := 0; reader < readers; reader++ {
						wg.Add(1)
						go func(seed int) {
							defer wg.Done()
							rng := mathrand.New(mathrand.NewPCG(uint64(seed), uint64(seed+1)))
							var ops int64
							for {
								select {
								case <-stop:
									atomic.AddInt64(&readOps, ops)
									return
								default:
									key := uint64(rng.IntN(populateSize))
									engine.Get(uint64Key(key))
									ops++
								}
							}
						}(reader * 2)
					}

					// Writers: sequential Puts to new keys (past populateSize)
					for writer := 0; writer < writers; writer++ {
						wg.Add(1)
						go func(base int) {
							defer wg.Done()
							var ops int64
							idx := base
							for {
								select {
								case <-stop:
									atomic.AddInt64(&writeOps, ops)
									return
								default:
									engine.Put(uint64Key(uint64(populateSize+idx)), nextVal(idx))
									idx += writers // stride to avoid key overlap
									ops++
								}
							}
						}(writer)
					}

					b.StartTimer()
					time.Sleep(duration)
					close(stop)
					wg.Wait()
					b.StopTimer()

					b.Logf("reads=%d (%d/s) writes=%d (%d/s)",
						readOps, readOps/int64(duration.Seconds()),
						writeOps, writeOps/int64(duration.Seconds()))
					engine.Close()
				}
			})

			b.Run(tag+"/Pebble", func(b *testing.B) {
				for iter := 0; iter < b.N; iter++ {
					b.StopTimer()
					db := initPebble(b)
					for idx := 0; idx < populateSize; idx++ {
						db.Set(uint64Key(uint64(idx)), nextVal(idx), pebble.NoSync)
					}
					db.Flush()

					var readOps, writeOps int64
					var wg sync.WaitGroup
					stop := make(chan struct{})

					for reader := 0; reader < readers; reader++ {
						wg.Add(1)
						go func(seed int) {
							defer wg.Done()
							rng := mathrand.New(mathrand.NewPCG(uint64(seed), uint64(seed+1)))
							var ops int64
							for {
								select {
								case <-stop:
									atomic.AddInt64(&readOps, ops)
									return
								default:
									key := uint64(rng.IntN(populateSize))
									v, closer, err := db.Get(uint64Key(key))
									if err == nil {
										_ = v
										closer.Close()
									}
									ops++
								}
							}
						}(reader * 2)
					}

					for writer := 0; writer < writers; writer++ {
						wg.Add(1)
						go func(base int) {
							defer wg.Done()
							var ops int64
							idx := base
							for {
								select {
								case <-stop:
									atomic.AddInt64(&writeOps, ops)
									return
								default:
									db.Set(uint64Key(uint64(populateSize+idx)), nextVal(idx), pebble.NoSync)
									idx += writers
									ops++
								}
							}
						}(writer)
					}

					b.StartTimer()
					time.Sleep(duration)
					close(stop)
					wg.Wait()
					b.StopTimer()

					b.Logf("reads=%d (%d/s) writes=%d (%d/s)",
						readOps, readOps/int64(duration.Seconds()),
						writeOps, writeOps/int64(duration.Seconds()))
					db.Close()
				}
			})
		}
	}
}

