package benchmark

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v4"
	_ "github.com/mattn/go-sqlite3"
	bolt "go.etcd.io/bbolt"

	"github.com/germtb/protodb"
)

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

func initBadger(b *testing.B) *badger.DB {
	b.Helper()
	opts := badger.DefaultOptions(filepath.Join(b.TempDir(), "badger"))
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
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

		// --- BadgerDB: N Puts in a transaction ---
		b.Run(fmt.Sprintf("Write%d/Badger", batchSize), func(b *testing.B) {
			db := initBadger(b)
			defer db.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				txn := db.NewTransaction(true)
				for idx := 0; idx < batchSize; idx++ {
					txn.Set(poolKey(iter*batchSize+idx), val)
				}
				if err := txn.Commit(); err != nil {
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

	b.Run("Get/Badger", func(b *testing.B) {
		db := initBadger(b)
		defer db.Close()
		txn := db.NewTransaction(true)
		for idx := 0; idx < populateSize; idx++ {
			txn.Set(uint64Key(uint64(idx)), val)
			if idx%500 == 0 {
				txn.Commit()
				txn = db.NewTransaction(true)
			}
		}
		txn.Commit()
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			txn := db.NewTransaction(false)
			item, err := txn.Get(poolKey(iter % populateSize))
			if err != nil {
				b.Fatal(err)
			}
			item.ValueCopy(nil)
			txn.Discard()
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

	b.Run("Scan1000/Badger", func(b *testing.B) {
		db := initBadger(b)
		defer db.Close()
		txn := db.NewTransaction(true)
		for idx := 0; idx < populateSize; idx++ {
			txn.Set(uint64Key(uint64(idx)), val)
			if idx%500 == 0 {
				txn.Commit()
				txn = db.NewTransaction(true)
			}
		}
		txn.Commit()
		lo := uint64Key(0)
		hi := uint64Key(1000)
		b.ResetTimer()
		for iter := 0; iter < b.N; iter++ {
			txn := db.NewTransaction(false)
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			count := 0
			for it.Seek(lo); it.Valid(); it.Next() {
				item := it.Item()
				if bytes.Compare(item.Key(), hi) >= 0 {
					break
				}
				item.ValueCopy(nil)
				count++
			}
			it.Close()
			txn.Discard()
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

	// --- Badger ---
	before = measure()
	opts := badger.DefaultOptions(filepath.Join(t.TempDir(), "badger"))
	opts.Logger = nil
	bdb, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	txn := bdb.NewTransaction(true)
	for idx := 0; idx < entryCount; idx++ {
		txn.Set(uint64Key(uint64(idx)), val)
		if idx%500 == 0 {
			txn.Commit()
			txn = bdb.NewTransaction(true)
		}
	}
	txn.Commit()
	badgerHeap := measure() - before
	t.Logf("Badger: %d KB heap (%d entries)", badgerHeap/1024, entryCount)
	bdb.Close()
}
