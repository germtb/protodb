package sidb

import (
	"encoding/json"
	"fmt"
	"path"
	"testing"
)

func TestInit(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()
	expectedDir := path.Join(append([]string{RootPath()}, namespace...)...)
	expectedPath := path.Join(expectedDir, name+".db")
	if db.Path != expectedPath {
		t.Errorf("Expected database path %s, got %s", expectedPath, db.Path)
	}
}

func TestBulkPut(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	if string(entry.Value) != "updated_data" {
		t.Errorf("Expected entry data %s after update, got %s", "updated_data", string(entry.Value))
	}
	if string(entry.Value) != "updated_data" {
		t.Errorf("Expected updated entry data %s, got %s", "updated_data", string(entry.Value))
	}
}

func TestDelete(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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

func TestUpdate(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Drop()

	entryType := "test_type"
	data := []byte("test_data")
	key := "test_key"
	err = db.Upsert(EntryInput{Type: entryType, Value: data, Key: key, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}

	newData := []byte("updated_data")
	err = db.Update(EntryInput{Type: entryType, Value: newData, Key: key, Grouping: ""})
	if err != nil {
		t.Fatalf("Failed to update entry: %v", err)
	}

	entry, err := db.Get(entryType, key)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if string(entry.Value) != string(newData) {
		t.Errorf("Expected entry data %s, got %s", string(newData), string(entry.Value))
	}
}

func TestClose(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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
	namespace := []string{"test_namespace"}
	name := "test_db"
	db, err := Init(namespace, name)
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

type testItem struct {
	Name  string
	Value int
}

func serializeTestItem(item testItem) ([]byte, error) {
	return json.Marshal(item)
}

func deserializeTestItem(data []byte) (testItem, error) {
	var item testItem
	err := json.Unmarshal(data, &item)
	return item, err
}

func TestStoreUpsertGetDelete(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_store_db"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore(db, "test_type", serializeTestItem, deserializeTestItem, nil, nil)

	item := testItem{Name: "one", Value: 1}
	input := StoreEntryInput[testItem]{Key: "key_1", Value: item}

	// --- Upsert ---
	if err := store.Upsert(input); err != nil {
		t.Fatalf("Failed to upsert: %v", err)
	}

	// --- Get ---
	got, err := store.Get("key_1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if got.Name != item.Name || got.Value != item.Value {
		t.Errorf("Expected %+v, got %+v", item, got)
	}

	// --- Update ---
	item.Value = 99
	input.Value = item
	if err := store.Upsert(input); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	got, err = store.Get("key_1")
	if err != nil {
		t.Fatalf("Failed to get after update: %v", err)
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
	MakeStore(db, "bulk_type", serializeTestItem, deserializeTestItem, nil, nil)

	inputs := []StoreEntryInput[testItem]{
		{Key: "key_a", Value: testItem{Name: "A", Value: 10}, Grouping: "g1"},
		{Key: "key_b", Value: testItem{Name: "B", Value: 20}, Grouping: "g1"},
		{Key: "key_c", Value: testItem{Name: "C", Value: 30}, Grouping: "g2"},
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

	expected := map[string]int{"A": 10, "B": 20, "C": 30}
	for _, got := range results {
		if expected[got.Name] != got.Value {
			t.Errorf("Unexpected value for %s: %d", got.Name, got.Value)
		}
	}
}

func TestStoreSerializationError(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_store_error"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	badSerializer := func(_ testItem) ([]byte, error) {
		return nil, fmt.Errorf("serialization failed intentionally")
	}

	store := MakeStore(db, "bad_store", badSerializer, deserializeTestItem, nil, nil)
	input := StoreEntryInput[testItem]{Key: "k", Value: testItem{Name: "bad"}}

	err = store.Upsert(input)
	if err == nil {
		t.Fatalf("Expected serialization error, got nil")
	}
}

func TestStoreWithSortingIndex(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_store_sorting"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	deriveSortingIndex := func(item testItem) int64 {
		return int64(item.Value)
	}

	store := MakeStore(db, "sort_store", serializeTestItem, deserializeTestItem, deriveSortingIndex, nil)

	items := []StoreEntryInput[testItem]{
		{Key: "k1", Value: testItem{Name: "Item1", Value: 30}},
		{Key: "k2", Value: testItem{Name: "Item2", Value: 10}},
		{Key: "k3", Value: testItem{Name: "Item3", Value: 20}},
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

type User struct {
	Name  string
	Email string
	Age   int
}

func serializeUser(u User) ([]byte, error) {
	return json.Marshal(u)
}

func deserializeUser(data []byte) (User, error) {
	var u User
	err := json.Unmarshal(data, &u)
	return u, err
}

func TestSecondaryIndexes(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_secondary_indexes"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	// Create store with secondary indexes
	store := MakeStore(db, "users", serializeUser, deserializeUser, nil,
		map[string]IndexExtractor[User]{
			"email": {
				Extract: func(u User) interface{} { return u.Email },
				Type:    StringIndex,
			},
			"age": {
				Extract: func(u User) interface{} { return u.Age },
				Type:    NumberIndex,
			},
		})

	// Insert test data
	users := []StoreEntryInput[User]{
		{Key: "u1", Value: User{Name: "Alice", Email: "alice@example.com", Age: 30}},
		{Key: "u2", Value: User{Name: "Bob", Email: "bob@example.com", Age: 25}},
		{Key: "u3", Value: User{Name: "Charlie", Email: "charlie@example.com", Age: 35}},
		{Key: "u4", Value: User{Name: "David", Email: "david@example.com", Age: 28}},
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
	// Should get Alice (30) and Charlie (35)
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
	// Should get Alice (30), Charlie (35), David (28)
	names = make(map[string]bool)
	for _, u := range results {
		names[u.Name] = true
	}
	if !names["Alice"] || !names["Charlie"] || !names["David"] {
		t.Errorf("Expected Alice, Charlie, and David, got %v", names)
	}
}

func TestQueryBuilder(t *testing.T) {
	namespace := []string{"test_namespace"}
	name := "test_query_builder"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	// Create store with indexes
	store := MakeStore(db, "users", serializeUser, deserializeUser, nil,
		map[string]IndexExtractor[User]{
			"email": {
				Extract: func(u User) interface{} { return u.Email },
				Type:    StringIndex,
			},
			"age": {
				Extract: func(u User) interface{} { return u.Age },
				Type:    NumberIndex,
			},
		})

	// Insert test data
	users := []StoreEntryInput[User]{
		{Key: "u1", Value: User{Name: "Alice", Email: "alice@example.com", Age: 30}},
		{Key: "u2", Value: User{Name: "Bob", Email: "bob@example.com", Age: 25}},
		{Key: "u3", Value: User{Name: "Charlie", Email: "charlie@example.com", Age: 35}},
		{Key: "u4", Value: User{Name: "David", Email: "david@example.com", Age: 28}},
		{Key: "u5", Value: User{Name: "Eve", Email: "eve@example.com", Age: 22}},
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
	// Should be ordered by sortingIndex ascending (which defaults to 0 for all since we removed timestamp)
	// So order is not guaranteed by sortingIndex, but the query should work

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
	namespace := []string{"test_namespace"}
	name := "test_index_update"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore(db, "users", serializeUser, deserializeUser, nil,
		map[string]IndexExtractor[User]{
			"age": {
				Extract: func(u User) interface{} { return u.Age },
				Type:    NumberIndex,
			},
		})

	// Insert user
	err = store.Upsert(StoreEntryInput[User]{
		Key:   "u1",
		Value: User{Name: "Alice", Email: "alice@example.com", Age: 30},
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
	err = store.Upsert(StoreEntryInput[User]{
		Key:   "u1",
		Value: User{Name: "Alice", Email: "alice@example.com", Age: 35},
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
	namespace := []string{"test_namespace"}
	name := "test_index_delete"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	store := MakeStore(db, "users", serializeUser, deserializeUser, nil,
		map[string]IndexExtractor[User]{
			"email": {
				Extract: func(u User) interface{} { return u.Email },
				Type:    StringIndex,
			},
		})

	// Insert user
	err = store.Upsert(StoreEntryInput[User]{
		Key:   "u1",
		Value: User{Name: "Alice", Email: "alice@example.com", Age: 30},
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
	namespace := []string{"test_namespace"}
	name := "test_no_duplicates"
	db, err := Init(namespace, name)
	if err != nil {
		t.Fatalf("Failed to init db: %v", err)
	}
	defer db.Drop()

	// Create store with multiple indexed fields
	store := MakeStore(db, "users", serializeUser, deserializeUser, nil,
		map[string]IndexExtractor[User]{
			"email": {
				Extract: func(u User) interface{} { return u.Email },
				Type:    StringIndex,
			},
			"age": {
				Extract: func(u User) interface{} { return u.Age },
				Type:    NumberIndex,
			},
		})

	// Insert test data - multiple users with various attributes
	users := []StoreEntryInput[User]{
		{Key: "u1", Value: User{Name: "Alice", Email: "alice@example.com", Age: 30}},
		{Key: "u2", Value: User{Name: "Bob", Email: "bob@example.com", Age: 30}},
		{Key: "u3", Value: User{Name: "Charlie", Email: "charlie@example.com", Age: 25}},
		{Key: "u4", Value: User{Name: "David", Email: "david@example.com", Age: 30}},
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
	// Verify uniqueness by checking keys
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
	// Verify uniqueness
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 2: Duplicate found for user %s", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 3: Multiple filters with LIMIT - duplicates could cause wrong results
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
	// Verify uniqueness
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 3: Duplicate found for user %s with LIMIT", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 4: Filters with OFFSET - duplicates could cause skipped/repeated results
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
	// Verify uniqueness
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 4: Duplicate found for user %s with OFFSET", u.Name)
		}
		seen[u.Name] = true
	}

	// Test 5: Range query with multiple filters on the SAME field
	// This is a critical edge case that could expose duplicate issues
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
	// Verify uniqueness - this is critical because we're JOINing the same index table twice
	seen = make(map[string]bool)
	for _, u := range results {
		if seen[u.Name] {
			t.Errorf("Test 5: CRITICAL - Duplicate found for user %s in range query on same field", u.Name)
		}
		seen[u.Name] = true
	}
	// Verify we got the right users
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
