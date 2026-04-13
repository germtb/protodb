package benchmark

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	_ "github.com/mattn/go-sqlite3"
	bolt "go.etcd.io/bbolt"

	"github.com/germtb/protodb"
)

func now() time.Time          { return time.Now() }
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

func initPebble(b *testing.B) *pebble.DB {
	b.Helper()
	db, err := pebble.Open(filepath.Join(b.TempDir(), "pebble"), &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func initPebbleT(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(filepath.Join(t.TempDir(), "pebble"), &pebble.Options{})
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
				if err := tx.Apply(); err != nil {
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
}

// TestLoadedPerformance measures Get, GetMiss, and Scan after populating
// ~500 MB of data. Uses Test (not Benchmark) to avoid re-populating per
// sub-benchmark. Each engine is populated once, then all ops are timed.
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
	t.Log("Loading LSM...")
	engine, _ := protodb.Open(t.TempDir())
	engine.SetPolicy(&protodb.Policy{
		FlushThreshold:      1024 * 1024 * 64,
		CompactionThreshold: 1000,
	})
	for idx := 0; idx < totalEntries; idx++ {
		engine.Put(uint64Key(uint64(idx)), val)
		if (idx+1)%batchSize == 0 {
			engine.Flush()
			engine.Compact()
		}
	}
	if totalEntries%batchSize != 0 {
		engine.Flush()
		engine.Compact()
	}
	l1Count, l1Size := engine.L1Stats()
	t.Logf("LSM loaded: %d SSTs, %.1f MB", l1Count, float64(l1Size)/(1024*1024))

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
	engine.Close()

	// --- Pebble ---
	t.Log("Loading Pebble...")
	pdb := initPebbleT(t)
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
	t.Log("Pebble loaded")

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
	pdb.Close()

	// --- Bolt ---
	t.Log("Loading Bolt...")
	bdb := initBoltT(t)
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
	t.Log("Bolt loaded")

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
	val := mustMarshal(b, makeItem(0))

	for _, entryCount := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("%d/LSM", entryCount), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				engine := initLSM(b)

				batchSize := 1000
				for idx := 0; idx < entryCount; idx++ {
					engine.Put(uint64Key(uint64(idx)), val)
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
					batch.Set(uint64Key(uint64(idx)), val, nil)
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
		CompactionThreshold: 1000,              // effectively disable auto-compact
	})

	val := make([]byte, 100) // 100-byte values
	const batchSize = 10000  // entries per flush (~1.2MB per batch)
	const rounds = 430       // ~500 MB final

	t.Logf("%-8s  %-12s  %-12s  %-10s  %-10s", "Round", "L1 SSTs", "L1 Size", "Compact ms", "Get ns")

	for round := 0; round < rounds; round++ {
		// Write a batch of entries with keys spread across the full range.
		// Use round*batchSize offset so data accumulates over time.
		for i := 0; i < batchSize; i++ {
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

		// Count L1 state
		l1SSTs, l1Size := engine.L1Stats()

		if round < 20 || round%10 == 9 {
			t.Logf("%-8d  %-12d  %-12s  %-10.1f  %-10d",
				round+1, l1SSTs, formatBytes(l1Size), float64(compactDur.Microseconds())/1000.0, getDur.Nanoseconds())
		}
	}
}

// BenchmarkBlockSize measures Get and Scan performance at different block sizes.
func BenchmarkBlockSize(b *testing.B) {
	val := mustMarshal(b, makeItem(0))
	const populateSize = 10000

	for _, blockSize := range []int{3800, 4096, 32768} {
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
		val[i] = byte(i % 26 + 'a') // compressible: repeating lowercase letters
	}

	dir := t.TempDir()
	engine, _ := protodb.Open(dir)
	engine.SetPolicy(&protodb.Policy{
		FlushThreshold:      1024 * 1024 * 64,
		CompactionThreshold: 1000,
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
		CompactionThreshold: 1000,
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
