package benchmark

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"

	"github.com/germtb/protodb"
	"github.com/germtb/protodb/internal/testpb"
)

// ------------------------------------------------------------
// helpers
// ------------------------------------------------------------

func initDB(b *testing.B) *protodb.Database {
	b.Helper()
	db, err := protodb.Init(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func initRawSQLite(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(b.TempDir(), "raw.db"))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		b.Fatal(err)
	}
	// Same schema as protodb.Init — apples-to-apples comparison
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS entries (
		"key" TEXT NOT NULL,
		"type" TEXT NOT NULL,
		"grouping" TEXT,
		"sortingIndex" INTEGER,
		"value" BLOB,
		PRIMARY KEY ("key", "type")
	) WITHOUT ROWID;

	CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(type);
	CREATE INDEX IF NOT EXISTS idx_entries_grouping ON entries(type, grouping);
	CREATE INDEX IF NOT EXISTS idx_entries_sorting_index ON entries(type, sortingIndex);

	CREATE TABLE IF NOT EXISTS entry_indexes (
		"entry_key" TEXT NOT NULL,
		"entry_type" TEXT NOT NULL,
		"field_name" TEXT NOT NULL,
		"field_value_str" TEXT,
		"field_value_num" REAL,
		PRIMARY KEY ("entry_key", "entry_type", "field_name"),
		FOREIGN KEY ("entry_key", "entry_type") REFERENCES entries("key", "type") ON DELETE CASCADE
	) WITHOUT ROWID;

	CREATE INDEX IF NOT EXISTS idx_index_type_field_str ON entry_indexes(entry_type, field_name, field_value_str);
	CREATE INDEX IF NOT EXISTS idx_index_type_field_num ON entry_indexes(entry_type, field_name, field_value_num);`)
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func makeItem(i int) *testpb.TestItem {
	return &testpb.TestItem{Name: fmt.Sprintf("item-%d", i), Value: int32(i)}
}

func makeUser(i int) *testpb.TestUser {
	return &testpb.TestUser{
		Name:  fmt.Sprintf("user-%d", i),
		Email: fmt.Sprintf("user-%d@example.com", i),
		Age:   int32(20 + i%50),
	}
}

func mustMarshal(b *testing.B, msg proto.Message) []byte {
	b.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}
	return data
}

// ------------------------------------------------------------
// 1. Overhead: Raw SQLite vs Store
// ------------------------------------------------------------

func BenchmarkOverhead(b *testing.B) {
	// --- Upsert ---
	b.Run("Upsert/Raw", func(b *testing.B) {
		raw := initRawSQLite(b)
		defer raw.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := makeItem(i)
			data, err := proto.Marshal(item)
			if err != nil {
				b.Fatal(err)
			}
			key := fmt.Sprintf("key-%d", i)
			_, err = raw.Exec(
				`INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?,?,?,?,?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex`,
				"testpb.TestItem", data, key, "", 0,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Upsert/Store", func(b *testing.B) {
		db := initDB(b)
		defer db.Drop()
		store := protodb.MakeStore[*testpb.TestItem](db, func(item *testpb.TestItem) int64 {
			return int64(item.Value)
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i)
			err := store.Upsert(protodb.StoreEntryInput[*testpb.TestItem]{
				Key:   key,
				Value: makeItem(i),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// --- Get ---
	b.Run("Get/Raw", func(b *testing.B) {
		raw := initRawSQLite(b)
		defer raw.Close()
		data := mustMarshal(b, makeItem(0))
		_, _ = raw.Exec(
			`INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?,?,?,?,?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex`,
			"testpb.TestItem", data, "target", "", 0,
		)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			row := raw.QueryRow("SELECT type, value, key, grouping, sortingIndex FROM entries WHERE type = ? AND key = ?",
				"testpb.TestItem", "target")
			var t, k, g string
			var v []byte
			var s int64
			if err := row.Scan(&t, &v, &k, &g, &s); err != nil {
				b.Fatal(err)
			}
			var out testpb.TestItem
			if err := proto.Unmarshal(v, &out); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get/Store", func(b *testing.B) {
		db := initDB(b)
		defer db.Drop()
		store := protodb.MakeStore[*testpb.TestItem](db, func(item *testpb.TestItem) int64 {
			return int64(item.Value)
		})
		_ = store.Upsert(protodb.StoreEntryInput[*testpb.TestItem]{
			Key: "target", Value: makeItem(0),
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := store.Get("target")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// --- BulkUpsert/100 ---
	b.Run("BulkUpsert100/Raw", func(b *testing.B) {
		raw := initRawSQLite(b)
		defer raw.Close()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			tx, err := raw.Begin()
			if err != nil {
				b.Fatal(err)
			}
			stmt, _ := tx.Prepare(`INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?,?,?,?,?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex`)
			for i := 0; i < 100; i++ {
				data, err := proto.Marshal(makeItem(i))
				if err != nil {
					b.Fatal(err)
				}
				key := fmt.Sprintf("key-%d-%d", n, i)
				if _, err := stmt.Exec("testpb.TestItem", data, key, "", int64(i)); err != nil {
					b.Fatal(err)
				}
			}
			stmt.Close()
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("BulkUpsert100/Store", func(b *testing.B) {
		db := initDB(b)
		defer db.Drop()
		store := protodb.MakeStore[*testpb.TestItem](db, func(item *testpb.TestItem) int64 {
			return int64(item.Value)
		})
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			entries := make([]protodb.StoreEntryInput[*testpb.TestItem], 100)
			for i := 0; i < 100; i++ {
				entries[i] = protodb.StoreEntryInput[*testpb.TestItem]{
					Key:   fmt.Sprintf("key-%d-%d", n, i),
					Value: makeItem(i),
				}
			}
			if err := store.BulkUpsert(entries); err != nil {
				b.Fatal(err)
			}
		}
	})

}

// ------------------------------------------------------------
// 2. Scaling: BulkUpsert and Query at various N
// ------------------------------------------------------------

func BenchmarkScaling(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("BulkUpsert/%d", n), func(b *testing.B) {
			db := initDB(b)
			defer db.Drop()
			store := protodb.MakeStore[*testpb.TestItem](db, func(item *testpb.TestItem) int64 {
				return int64(item.Value)
			})
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				entries := make([]protodb.StoreEntryInput[*testpb.TestItem], n)
				for i := 0; i < n; i++ {
					entries[i] = protodb.StoreEntryInput[*testpb.TestItem]{
						Key:   fmt.Sprintf("key-%d-%d", iter, i),
						Value: makeItem(i),
					}
				}
				if err := store.BulkUpsert(entries); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("Query/%d", n), func(b *testing.B) {
			db := initDB(b)
			defer db.Drop()
			store := protodb.MakeStore[*testpb.TestItem](db, func(item *testpb.TestItem) int64 {
				return int64(item.Value)
			})
			entries := make([]protodb.StoreEntryInput[*testpb.TestItem], n)
			for i := 0; i < n; i++ {
				entries[i] = protodb.StoreEntryInput[*testpb.TestItem]{
					Key:   fmt.Sprintf("key-%d", i),
					Value: makeItem(i),
				}
			}
			if err := store.BulkUpsert(entries); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := store.Query().Exec()
				if err != nil {
					b.Fatal(err)
				}
				if len(results) != n {
					b.Fatalf("expected %d results, got %d", n, len(results))
				}
			}
		})
	}
}

// ------------------------------------------------------------
// 3. Secondary Index Cost
// ------------------------------------------------------------

func BenchmarkIndexes(b *testing.B) {
	// Upsert with 0 indexes vs 2 indexes
	b.Run("Upsert/NoIndexes", func(b *testing.B) {
		db := initDB(b)
		defer db.Drop()
		store := protodb.MakeStore[*testpb.TestUser](db, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := store.Upsert(protodb.StoreEntryInput[*testpb.TestUser]{
				Key:   fmt.Sprintf("u-%d", i),
				Value: makeUser(i),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Upsert/2Indexes", func(b *testing.B) {
		db := initDB(b)
		defer db.Drop()
		store := protodb.MakeStore[*testpb.TestUser](db, nil, "email", "age")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := store.Upsert(protodb.StoreEntryInput[*testpb.TestUser]{
				Key:   fmt.Sprintf("u-%d", i),
				Value: makeUser(i),
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Query with 0, 1, 2 filters from a 1000-row table
	const tableSize = 1000

	populateUsers := func(b *testing.B, indexFields ...string) *protodb.Store[*testpb.TestUser] {
		b.Helper()
		db := initDB(b)
		b.Cleanup(func() { db.Drop() })
		store := protodb.MakeStore[*testpb.TestUser](db, nil, indexFields...)
		entries := make([]protodb.StoreEntryInput[*testpb.TestUser], tableSize)
		for i := 0; i < tableSize; i++ {
			entries[i] = protodb.StoreEntryInput[*testpb.TestUser]{
				Key:   fmt.Sprintf("u-%d", i),
				Value: makeUser(i),
			}
		}
		if err := store.BulkUpsert(entries); err != nil {
			b.Fatal(err)
		}
		return store
	}

	b.Run("Query/0Filters", func(b *testing.B) {
		store := populateUsers(b)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := store.Query().Exec()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query/1Filter", func(b *testing.B) {
		store := populateUsers(b, "email", "age")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := store.Query().Where("age", protodb.OpGreaterThan, 40).Exec()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Query/2Filters", func(b *testing.B) {
		store := populateUsers(b, "email", "age")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := store.Query().
				Where("age", protodb.OpGreaterThan, 40).
				Where("email", protodb.OpLike, "%example.com").
				Exec()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ------------------------------------------------------------
// 4. Concurrent Reads
// ------------------------------------------------------------

func BenchmarkConcurrentReads(b *testing.B) {
	db := initDB(b)
	defer db.Drop()
	store := protodb.MakeStore[*testpb.TestItem](db, func(item *testpb.TestItem) int64 {
		return int64(item.Value)
	})

	const n = 1000
	entries := make([]protodb.StoreEntryInput[*testpb.TestItem], n)
	for i := 0; i < n; i++ {
		entries[i] = protodb.StoreEntryInput[*testpb.TestItem]{
			Key:   fmt.Sprintf("key-%d", i),
			Value: makeItem(i),
		}
	}
	if err := store.BulkUpsert(entries); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%n)
			_, err := store.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
