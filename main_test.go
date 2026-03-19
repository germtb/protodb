package protodb

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/germtb/protodb/internal/testpb"
)

func TestInit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Init(dbPath)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()
	if db.Path != dbPath {
		t.Errorf("Expected database path %s, got %s", dbPath, db.Path)
	}
}

func TestBulkPut(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	err = db.BulkUpsert([]EntryInput{{Type: entryType, Value: data, Key: "test_key", Grouping: ""}})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entries, err := db.Query(QueryParams{Type: &entryType})
	if err != nil {
		t.Fatalf("Failed to load entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Type != entryType {
		t.Errorf("Expected entry type %s, got %s", entryType, entries[0].Type)
	}
	if string(entries[0].Value) != string(data) {
		t.Errorf("Expected entry data %s, got %s", string(data), string(entries[0].Value))
	}
}

func TestGet(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: "test_key", Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entry, err := db.Get(entryType, "test_key")
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if entry.Key != "test_key" {
		t.Errorf("Expected entry key %s, got %s", "test_key", entry.Key)
	}
	if entry.Type != entryType {
		t.Errorf("Expected entry type %s, got %s", entryType, entry.Type)
	}
	if string(entry.Value) != string(data) {
		t.Errorf("Expected entry data %s, got %s", string(data), string(entry.Value))
	}
}

func TestUpsert(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: "test_key", Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entry, err := db.Get(entryType, "test_key")
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if entry.Key != "test_key" {
		t.Errorf("Expected entry key %s, got %s", "test_key", entry.Key)
	}

	if entry.Type != entryType {
		t.Errorf("Expected entry type %s, got %s", entryType, entry.Type)
	}

	if string(entry.Value) != string(data) {
		t.Errorf("Expected entry data %s, got %s", string(data), string(entry.Value))
	}

	err = db.Upsert(EntryInput{Type: entryType, Value: []byte("updated_data"), Key: "test_key", Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to update entry: %v", err)
	}

	entry, err = db.Get(entryType, "test_key")
	if err != nil {
		t.Fatalf("Failed to get entry after update: %v", err)
	}

	if string(entry.Value) != "updated_data" {
		t.Errorf("Expected updated entry data %s, got %s", "updated_data", string(entry.Value))
	}
	if entry.Key != "test_key" {
		t.Errorf("Expected entry key %s after update, got %s", "test_key", entry.Key)
	}
	if entry.Type != entryType {
		t.Errorf("Expected entry type %s after update, got %s", entryType, entry.Type)
	}
}

func TestDelete(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: "test_key", Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	err = db.Delete(entryType, "test_key")
	if err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	entry, err := db.Get(entryType, "test_key")

	if err != nil {
		t.Fatalf("Error occurred while getting deleted entry: %v", err)
	}

	if entry != nil {
		t.Errorf("Expected nil entry after deletion, got %+v", entry)
	}
}

func TestBulkDelete(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	err = db.BulkUpsert([]EntryInput{
		{Type: entryType, Value: data, Key: "test_key_1"},
		{Type: entryType, Value: data, Key: "test_key_2"},
		{Type: entryType, Value: data, Key: "test_key_3"},
	})
	if err != nil {
		t.Fatalf("Failed to put entries: %v", err)
	}

	err = db.BulkDelete(entryType, []string{"test_key_1", "test_key_2"})
	if err != nil {
		t.Fatalf("Failed to delete entries by grouping: %v", err)
	}

	entries, err := db.Query(QueryParams{Type: &entryType})
	if err != nil {
		t.Fatalf("Failed to load entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry after bulk delete, got %d", len(entries))
	}

	if entries[0].Key != "test_key_3" {
		t.Errorf("Expected remaining entry key to be 'test_key_3', got '%s'", entries[0].Key)
	}
}

func TestQuery(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	type_1 := "type_1"
	type_2 := "type_2"

	err = db.BulkUpsert([]EntryInput{
		{Type: type_1, Value: []byte("data_1"), Key: "key_1", Grouping: ""},
		{Type: type_2, Value: []byte("data_2"), Key: "key_2", Grouping: ""},
		{Type: type_1, Value: []byte("data_3"), Key: "key_3", Grouping: ""},
	})

	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	entries, err := db.Query(QueryParams{
		Type: &type_1,
	})

	if err != nil {
		t.Fatalf("Failed to query entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].Type != "type_1" || string(entries[0].Value) != "data_1" {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}
	if entries[1].Type != "type_1" || string(entries[1].Value) != "data_3" {
		t.Errorf("Unexpected entry: %+v", entries[1])
	}

	entries, err = db.Query(QueryParams{
		Type: &type_2,
	})

	if err != nil {
		t.Fatalf("Failed to query entries: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Type != "type_2" || string(entries[0].Value) != "data_2" {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}
}

func TestQueryWithLimitOffset(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	type_1 := "type_1"
	err = db.BulkUpsert([]EntryInput{
		{Type: type_1, Value: []byte("data_1"), Key: "key_1"},
		{Type: type_1, Value: []byte("data_2"), Key: "key_2"},
		{Type: type_1, Value: []byte("data_3"), Key: "key_3"},
		{Type: type_1, Value: []byte("data_4"), Key: "key_4"},
	})
	if err != nil {
		t.Fatalf("Failed to put entries: %v", err)
	}

	limit := 2
	offset := 1
	entries, err := db.Query(QueryParams{
		Type:   &type_1,
		Limit:  &limit,
		Offset: &offset,
	})
	if err != nil {
		t.Fatalf("Failed to query entries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].Type != "type_1" || string(entries[0].Value) != "data_2" {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}
	if entries[1].Type != "type_1" || string(entries[1].Value) != "data_3" {
		t.Errorf("Unexpected entry: %+v", entries[1])
	}
}

func TestClose(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Attempting to use the database after closing should result in an error
	_, err = db.Query(QueryParams{Type: ptr("test_type")})
	if err == nil {
		t.Fatalf("Expected error when using closed database, got nil")
	}
}

func TestGetByGrouping(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	grouping := "test_group"
	entryType := "test_type"
	data1 := []byte("test_data_1")
	data2 := []byte("test_data_2")

	err = db.Upsert(EntryInput{Type: entryType, Value: data1, Key: "test_key_1", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 1: %v", err)
	}

	err = db.Upsert(EntryInput{Type: entryType, Value: data2, Key: "test_key_2", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 2: %v", err)
	}

	entries, err := db.Query(QueryParams{Type: &entryType, Grouping: &grouping})
	if err != nil {
		t.Fatalf("Failed to get entries by grouping: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	if entries[0].Type != "test_type" || string(entries[0].Value) != string(data1) {
		t.Errorf("Unexpected entry: %+v", entries[0])
	}
	if entries[1].Type != "test_type" || string(entries[1].Value) != string(data2) {
		t.Errorf("Unexpected entry: %+v", entries[1])
	}
}

func TestDeleteByGrouping(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	grouping := "test_group"
	entryType := "test_type"
	data1 := []byte("test_data_1")
	data2 := []byte("test_data_2")
	err = db.Upsert(EntryInput{Type: entryType, Value: data1, Key: "test_key_1", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 1: %v", err)
	}
	err = db.Upsert(EntryInput{Type: entryType, Value: data2, Key: "test_key_2", Grouping: grouping})
	if err != nil {
		t.Fatalf("Failed to put entry 2: %v", err)
	}

	err = db.DeleteByGrouping(entryType, grouping)
	if err != nil {
		t.Fatalf("Failed to delete entries by grouping: %v", err)
	}

	entries, err := db.Query(QueryParams{Type: &entryType, Grouping: &grouping})
	if err != nil {
		t.Fatalf("Failed to get entries by grouping: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("Expected 0 entries after deletion, got %d", len(entries))
	}
}

func ptr[T any](v T) *T {
	return &v
}

func TestQueryWithSortingIndex(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	err = db.BulkUpsert([]EntryInput{
		{Type: entryType, Value: []byte("data_1"), Key: "key_1", SortingIndex: int64(2)},
		{Type: entryType, Value: []byte("data_2"), Key: "key_2", SortingIndex: int64(1)},
		{Type: entryType, Value: []byte("data_3"), Key: "key_3", SortingIndex: int64(3)},
	})
	if err != nil {
		t.Fatalf("Failed to put entries: %v", err)
	}

	entries, err := db.Query(QueryParams{
		Type:      &entryType,
		SortOrder: Ascending,
	})

	if err != nil {
		t.Fatalf("Failed to query entries: %v", err)
	}

	if entries[0].Key != "key_2" || string(entries[0].Value) != "data_2" {
		t.Errorf("Unexpected first entry: %+v", entries[0])
	}
	if entries[1].Key != "key_1" || string(entries[1].Value) != "data_1" {
		t.Errorf("Unexpected second entry: %+v", entries[1])
	}
	if entries[2].Key != "key_3" || string(entries[2].Value) != "data_3" {
		t.Errorf("Unexpected third entry: %+v", entries[2])
	}

	entriesDesc, err := db.Query(QueryParams{
		Type:      &entryType,
		SortOrder: Descending,
	})

	if err != nil {
		t.Fatalf("Failed to query entries in descending order: %v", err)
	}

	if entriesDesc[0].Key != "key_3" || string(entriesDesc[0].Value) != "data_3" {
		t.Errorf("Unexpected first entry in desc order: %+v", entriesDesc[0])
	}
	if entriesDesc[1].Key != "key_1" || string(entriesDesc[1].Value) != "data_1" {
		t.Errorf("Unexpected second entry in desc order: %+v", entriesDesc[1])
	}
	if entriesDesc[2].Key != "key_2" || string(entriesDesc[2].Value) != "data_2" {
		t.Errorf("Unexpected third entry in desc order: %+v", entriesDesc[2])
	}
}

func TestCount(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	err = db.BulkUpsert([]EntryInput{
		{Type: entryType, Value: []byte("data_1"), Key: "key_1"},
		{Type: entryType, Value: []byte("data_2"), Key: "key_2"},
		{Type: entryType, Value: []byte("data_3"), Key: "key_3"},
	})
	if err != nil {
		t.Fatalf("Failed to put entries: %v", err)
	}

	count, err := db.Count()
	if err != nil {
		t.Fatalf("Failed to count entries: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

func TestStoreUpsertGetDelete(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestItem](db, nil)

	item := &testpb.TestItem{Name: "one", Value: 1}
	input := StoreEntryInput[*testpb.TestItem]{Key: "key_1", Value: item}

	// --- Upsert ---
	if err := store.Upsert(input); err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	// --- Get ---
	got, err := store.Get("key_1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if got == nil {
		t.Fatalf("Expected non-nil result from Get")
	}
	if got.Name != item.Name || got.Value != item.Value {
		t.Errorf("Expected %+v, got %+v", item, got)
	}

	// --- Update ---
	item2 := &testpb.TestItem{Name: "one", Value: 99}
	input2 := StoreEntryInput[*testpb.TestItem]{Key: "key_1", Value: item2}
	if err := store.Upsert(input2); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	got, err = store.Get("key_1")
	if err != nil {
		t.Fatalf("Failed to get after update: %v", err)
	}
	if got == nil {
		t.Fatalf("Expected non-nil result after update")
	}
	if got.Value != 99 {
		t.Errorf("Expected updated Value=99, got %d", got.Value)
	}

	// --- Delete ---
	if err := store.Delete("key_1"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	got, err = store.Get("key_1")
	if err != nil {
		t.Fatalf("Get after delete returned error: %v", err)
	}
	if got != nil {
		t.Errorf("Expected nil after delete, got %+v", got)
	}

	// --- BulkUpsert + Query ---
	inputs := []StoreEntryInput[*testpb.TestItem]{
		{Key: "key_a", Value: &testpb.TestItem{Name: "A", Value: 10}, Grouping: "g1"},
		{Key: "key_b", Value: &testpb.TestItem{Name: "B", Value: 20}, Grouping: "g1"},
		{Key: "key_c", Value: &testpb.TestItem{Name: "C", Value: 30}, Grouping: "g2"},
	}

	if err := store.BulkUpsert(inputs); err != nil {
		t.Fatalf("Failed BulkUpsert: %v", err)
	}

	results, err := store.Query().Exec()
	if err != nil {
		t.Fatalf("Failed Query(): %v", err)
	}

	if len(results) != len(inputs) {
		t.Fatalf("Expected %d items, got %d", len(inputs), len(results))
	}

	expected := map[string]int32{"A": 10, "B": 20, "C": 30}
	for _, got := range results {
		if expected[got.Name] != got.Value {
			t.Errorf("Unexpected value for %s: %d", got.Name, got.Value)
		}
	}
}

func TestStoreGetNotFound(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestItem](db, nil)
	got, err := store.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got != nil {
		t.Errorf("Expected nil for nonexistent key, got %+v", got)
	}
}

func TestStoreWithSortingIndex(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore(db, func(item *testpb.TestItem) int64 {
		return int64(item.Value)
	})

	items := []StoreEntryInput[*testpb.TestItem]{
		{Key: "k1", Value: &testpb.TestItem{Name: "Item1", Value: 30}},
		{Key: "k2", Value: &testpb.TestItem{Name: "Item2", Value: 10}},
		{Key: "k3", Value: &testpb.TestItem{Name: "Item3", Value: 20}},
	}

	if err := store.BulkUpsert(items); err != nil {
		t.Fatalf("Failed BulkUpsert: %v", err)
	}
	results, err := store.Query().SortOrder(Ascending).Exec()
	if err != nil {
		t.Fatalf("Failed Query(): %v", err)
	}
	if len(results) != len(items) {
		t.Fatalf("Expected %d items, got %d", len(items), len(results))
	}
	expectedOrder := []string{"Item2", "Item3", "Item1"}
	for i, got := range results {
		if got.Name != expectedOrder[i] {
			t.Errorf("At index %d, expected %s, got %s", i, expectedOrder[i], got.Name)
		}
	}
}

func TestSecondaryIndexes(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestUser](db, nil, "email", "age")

	users := []StoreEntryInput[*testpb.TestUser]{
		{Key: "u1", Value: &testpb.TestUser{Name: "Alice", Email: "alice@example.com", Age: 30}},
		{Key: "u2", Value: &testpb.TestUser{Name: "Bob", Email: "bob@example.com", Age: 25}},
		{Key: "u3", Value: &testpb.TestUser{Name: "Charlie", Email: "charlie@example.com", Age: 35}},
		{Key: "u4", Value: &testpb.TestUser{Name: "David", Email: "david@example.com", Age: 28}},
	}

	err = store.BulkUpsert(users)
	if err != nil {
		t.Fatalf("Failed to bulk upsert: %v", err)
	}

	// Test numeric filter: age > 28
	results, err := store.Query().Where("age", OpGreaterThan, 28).Exec()
	if err != nil {
		t.Fatalf("Failed to query with age filter: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
	names := make(map[string]bool)
	for _, u := range results {
		names[u.Name] = true
	}
	if !names["Alice"] || !names["Charlie"] {
		t.Errorf("Expected Alice and Charlie, got %v", names)
	}

	// Test string filter: email = bob@example.com
	results, err = store.Query().Where("email", OpEqual, "bob@example.com").Exec()
	if err != nil {
		t.Fatalf("Failed to query with email filter: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].Name != "Bob" {
		t.Errorf("Expected Bob, got %s", results[0].Name)
	}

	// Test multiple filters: age >= 28 AND email LIKE %example.com
	results, err = store.Query().
		Where("age", OpGreaterThanOrEqual, 28).
		Where("email", OpLike, "%example.com").
		Exec()
	if err != nil {
		t.Fatalf("Failed to query with multiple filters: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
	names = make(map[string]bool)
	for _, u := range results {
		names[u.Name] = true
	}
	if !names["Alice"] || !names["Charlie"] || !names["David"] {
		t.Errorf("Expected Alice, Charlie, and David, got %v", names)
	}
}

func TestQueryBuilder(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestUser](db, nil, "email", "age")

	users := []StoreEntryInput[*testpb.TestUser]{
		{Key: "u1", Value: &testpb.TestUser{Name: "Alice", Email: "alice@example.com", Age: 30}},
		{Key: "u2", Value: &testpb.TestUser{Name: "Bob", Email: "bob@example.com", Age: 25}},
		{Key: "u3", Value: &testpb.TestUser{Name: "Charlie", Email: "charlie@example.com", Age: 35}},
		{Key: "u4", Value: &testpb.TestUser{Name: "David", Email: "david@example.com", Age: 28}},
		{Key: "u5", Value: &testpb.TestUser{Name: "Eve", Email: "eve@example.com", Age: 22}},
	}

	err = store.BulkUpsert(users)
	if err != nil {
		t.Fatalf("Failed to bulk upsert: %v", err)
	}

	// Test query builder with Where and Limit
	results, err := store.Query().
		Where("age", OpGreaterThan, 25).
		Limit(2).
		Exec()
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results with limit, got %d", len(results))
	}

	// Test query builder with multiple Where clauses
	results, err = store.Query().
		Where("age", OpLessThan, 30).
		Where("age", OpGreaterThan, 23).
		Exec()
	if err != nil {
		t.Fatalf("Failed to execute query with multiple filters: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results (Bob 25, David 28), got %d", len(results))
	}

	// Test OrderBy ascending
	results, err = store.Query().
		Where("age", OpGreaterThanOrEqual, 25).
		SortOrder(Ascending).
		Exec()
	if err != nil {
		t.Fatalf("Failed to execute query with ordering: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("Expected at least 2 results, got %d", len(results))
	}

	// Test Offset
	results, err = store.Query().
		Offset(2).
		Limit(2).
		Exec()
	if err != nil {
		t.Fatalf("Failed to execute query with offset: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 results with offset and limit, got %d", len(results))
	}
}

func TestIndexUpdate(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestUser](db, nil, "age")

	// Insert user
	err = store.Upsert(StoreEntryInput[*testpb.TestUser]{
		Key:   "u1",
		Value: &testpb.TestUser{Name: "Alice", Email: "alice@example.com", Age: 30},
	})
	if err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	// Query by age
	results, err := store.Query().Where("age", OpEqual, 30).Exec()
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	// Update user age
	err = store.Upsert(StoreEntryInput[*testpb.TestUser]{
		Key:   "u1",
		Value: &testpb.TestUser{Name: "Alice", Email: "alice@example.com", Age: 35},
	})
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Old age query should return nothing
	results, err = store.Query().Where("age", OpEqual, 30).Exec()
	if err != nil {
		t.Fatalf("Failed to query after update: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for old age, got %d", len(results))
	}

	// New age query should return the user
	results, err = store.Query().Where("age", OpEqual, 35).Exec()
	if err != nil {
		t.Fatalf("Failed to query for new age: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result for new age, got %d", len(results))
	}
	if results[0].Age != 35 {
		t.Errorf("Expected age 35, got %d", results[0].Age)
	}
}

func TestIndexDelete(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestUser](db, nil, "email")

	// Insert user
	err = store.Upsert(StoreEntryInput[*testpb.TestUser]{
		Key:   "u1",
		Value: &testpb.TestUser{Name: "Alice", Email: "alice@example.com", Age: 30},
	})
	if err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	// Verify index exists
	results, err := store.Query().Where("email", OpEqual, "alice@example.com").Exec()
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result before delete, got %d", len(results))
	}

	// Delete user
	err = store.Delete("u1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify index is gone (CASCADE should have deleted it)
	results, err = store.Query().Where("email", OpEqual, "alice@example.com").Exec()
	if err != nil {
		t.Fatalf("Failed to query after delete: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results after delete, got %d", len(results))
	}
}

// TestNoDuplicatesWithSecondaryIndexes tests the typical SQLite issue where
// queries with JOINs on secondary indexes can return duplicate entities
func TestNoDuplicatesWithSecondaryIndexes(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestUser](db, nil, "email", "age")

	users := []StoreEntryInput[*testpb.TestUser]{
		{Key: "u1", Value: &testpb.TestUser{Name: "Alice", Email: "alice@example.com", Age: 30}},
		{Key: "u2", Value: &testpb.TestUser{Name: "Bob", Email: "bob@example.com", Age: 30}},
		{Key: "u3", Value: &testpb.TestUser{Name: "Charlie", Email: "charlie@example.com", Age: 25}},
		{Key: "u4", Value: &testpb.TestUser{Name: "David", Email: "david@example.com", Age: 30}},
	}

	err = store.BulkUpsert(users)
	if err != nil {
		t.Fatalf("Failed to bulk upsert: %v", err)
	}

	// Test 1: Single filter - should not produce duplicates
	results, err := store.Query().Where("age", OpEqual, 30).Exec()
	if err != nil {
		t.Fatalf("Failed to query with single filter: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Test 1: Expected 3 unique results, got %d", len(results))
	}
	seen := make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 1: Duplicate found for user %s", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 2: Multiple filters on same entry - critical test for duplicates
	results, err = store.Query().
		Where("age", OpEqual, 30).
		Where("email", OpLike, "%example.com").
		Exec()
	if err != nil {
		t.Fatalf("Failed to query with multiple filters: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Test 2: Expected 3 unique results with multiple filters, got %d", len(results))
	}
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 2: Duplicate found for user %s", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 3: Multiple filters with LIMIT
	results, err = store.Query().
		Where("age", OpGreaterThanOrEqual, 25).
		Where("email", OpLike, "%example.com").
		Limit(2).
		Exec()
	if err != nil {
		t.Fatalf("Failed to query with filters and limit: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Test 3: Expected exactly 2 results with LIMIT, got %d", len(results))
	}
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 3: Duplicate found for user %s with LIMIT", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 4: Filters with OFFSET
	results, err = store.Query().
		Where("age", OpGreaterThanOrEqual, 25).
		Offset(1).
		Limit(2).
		Exec()
	if err != nil {
		t.Fatalf("Failed to query with filters, offset and limit: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Test 4: Expected exactly 2 results with OFFSET and LIMIT, got %d", len(results))
	}
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 4: Duplicate found for user %s with OFFSET", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 5: Range query with multiple filters on the SAME field
	results, err = store.Query().
		Where("age", OpGreaterThan, 25).
		Where("age", OpLessThanOrEqual, 30).
		Exec()
	if err != nil {
		t.Fatalf("Failed to query with range filters: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Test 5: Expected 3 unique results with range, got %d", len(results))
	}
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 5: CRITICAL - Duplicate found for user %s in range query on same field", u.Name)
		}
		seen[u.Name] = true
	}
	expectedNames := map[string]bool{"Alice": true, "Bob": true, "David": true}
	for _, u := range results {
		if !expectedNames[u.Name] {
			t.Errorf("Test 5: Unexpected user %s in range 25 < age <= 30", u.Name)
		}
	}

	// Test 6: Count total to ensure no extra rows
	allResults, err := store.Query().Exec()
	if err != nil {
		t.Fatalf("Failed to query all: %v", err)
	}
	if len(allResults) != 4 {
		t.Errorf("Test 6: Expected 4 total users, got %d", len(allResults))
	}
}

func TestInitNonexistentDirectory(t *testing.T) {
	db, err := Init("/nonexistent/path/test.db")
	if err == nil {
		db.Close()
		t.Fatal("Expected error when opening database in nonexistent directory, got nil")
	}
}

func TestUpsertReplacesExistingKey(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	err = db.Upsert(EntryInput{Type: entryType, Key: "k1", Value: []byte("first")})
	if err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	err = db.Upsert(EntryInput{Type: entryType, Key: "k1", Value: []byte("second")})
	if err != nil {
		t.Fatalf("Failed to upsert duplicate key: %v", err)
	}

	entry, err := db.Get(entryType, "k1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if string(entry.Value) != "second" {
		t.Errorf("Expected value 'second' after replace, got '%s'", string(entry.Value))
	}

	count, err := db.Count()
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 entry after duplicate upsert, got %d", count)
	}
}

func TestGetNonexistentKey(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	entry, err := db.Get("test_type", "nonexistent")
	if err != nil {
		t.Fatalf("Get returned error for nonexistent key: %v", err)
	}
	if entry != nil {
		t.Errorf("Expected nil for nonexistent key, got %+v", entry)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}

	db.Close()

	if err := db.Upsert(EntryInput{Type: "t", Key: "k", Value: []byte("v")}); err == nil {
		t.Error("Expected error on Upsert after Close")
	}
	if _, err := db.Get("t", "k"); err == nil {
		t.Error("Expected error on Get after Close")
	}
	if err := db.Delete("t", "k"); err == nil {
		t.Error("Expected error on Delete after Close")
	}
}

func TestDeleteNonexistentKey(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	// Deleting a key that doesn't exist should not error
	err = db.Delete("test_type", "nonexistent")
	if err != nil {
		t.Errorf("Expected no error deleting nonexistent key, got %v", err)
	}
}

func TestDatabaseExists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	if DatabaseExists(dbPath) {
		t.Error("Expected DatabaseExists to return false before Init")
	}

	db, err := Init(dbPath)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}

	if !DatabaseExists(dbPath) {
		t.Error("Expected DatabaseExists to return true after Init")
	}

	db.Drop()

	if DatabaseExists(dbPath) {
		t.Error("Expected DatabaseExists to return false after Drop")
	}
}

func TestConcurrentAccess(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestItem](db, nil)

	const numWriters = 10
	const numReaders = 10
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	errCh := make(chan error, (numWriters+numReaders)*opsPerGoroutine)

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("w%d-k%d", writerID, i)
				err := store.Upsert(StoreEntryInput[*testpb.TestItem]{
					Key:   key,
					Value: &testpb.TestItem{Name: key, Value: int32(i)},
				})
				if err != nil {
					errCh <- fmt.Errorf("writer %d op %d: %w", writerID, i, err)
					return
				}
			}
		}(w)
	}

	// Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				_, err := store.Query().Exec()
				if err != nil {
					errCh <- fmt.Errorf("reader %d op %d: %w", readerID, i, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Concurrent error: %v", err)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	expected := int64(numWriters * opsPerGoroutine)
	if count != expected {
		t.Errorf("Expected %d entries, got %d", expected, count)
	}
}

func TestStoreBulkGet(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore[*testpb.TestItem](db, nil)

	// Insert some items
	items := []StoreEntryInput[*testpb.TestItem]{
		{Key: "k1", Value: &testpb.TestItem{Name: "Item1", Value: 10}},
		{Key: "k2", Value: &testpb.TestItem{Name: "Item2", Value: 20}},
		{Key: "k3", Value: &testpb.TestItem{Name: "Item3", Value: 30}},
	}
	if err := store.BulkUpsert(items); err != nil {
		t.Fatalf("Failed to bulk upsert: %v", err)
	}

	// BulkGet with existing keys
	result, err := store.BulkGet([]string{"k1", "k3"})
	if err != nil {
		t.Fatalf("Failed BulkGet: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(result))
	}
	if result["k1"].Name != "Item1" {
		t.Errorf("Expected Item1, got %s", result["k1"].Name)
	}
	if result["k3"].Name != "Item3" {
		t.Errorf("Expected Item3, got %s", result["k3"].Name)
	}

	// BulkGet with some missing keys
	result, err = store.BulkGet([]string{"k1", "missing1", "k2", "missing2"})
	if err != nil {
		t.Fatalf("Failed BulkGet with missing keys: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 results (missing keys ignored), got %d", len(result))
	}

	// BulkGet with empty slice
	result, err = store.BulkGet([]string{})
	if err != nil {
		t.Fatalf("Failed BulkGet with empty slice: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Expected 0 results for empty slice, got %d", len(result))
	}
}

func TestLargeBulkOperations(t *testing.T) {
	db, err := Init(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore(db, func(item *testpb.TestItem) int64 {
		return int64(item.Value)
	})

	const n = 600 // exceeds maxSQLPlaceholders (500)

	// BulkUpsert 600 items
	inputs := make([]StoreEntryInput[*testpb.TestItem], n)
	for i := 0; i < n; i++ {
		inputs[i] = StoreEntryInput[*testpb.TestItem]{
			Key:   fmt.Sprintf("key-%04d", i),
			Value: &testpb.TestItem{Name: fmt.Sprintf("Item%d", i), Value: int32(i)},
		}
	}
	if err := store.BulkUpsert(inputs); err != nil {
		t.Fatalf("Failed BulkUpsert of %d items: %v", n, err)
	}

	count, err := store.Count()
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != int64(n) {
		t.Fatalf("Expected %d entries after BulkUpsert, got %d", n, count)
	}

	// BulkGet all 600 keys
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	result, err := store.BulkGet(keys)
	if err != nil {
		t.Fatalf("Failed BulkGet of %d keys: %v", n, err)
	}
	if len(result) != n {
		t.Errorf("Expected %d results from BulkGet, got %d", n, len(result))
	}

	// Verify cross-chunk boundary values (items 499 and 500)
	if result["key-0499"].Name != "Item499" {
		t.Errorf("Cross-chunk boundary: expected Item499, got %s", result["key-0499"].Name)
	}
	if result["key-0500"].Name != "Item500" {
		t.Errorf("Cross-chunk boundary: expected Item500, got %s", result["key-0500"].Name)
	}

	// BulkDelete all 600 keys
	if err := store.BulkDelete(keys); err != nil {
		t.Fatalf("Failed BulkDelete of %d keys: %v", n, err)
	}

	count, err = store.Count()
	if err != nil {
		t.Fatalf("Failed to count after BulkDelete: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 entries after BulkDelete, got %d", count)
	}
}
