package sidb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// This package is the Si(mple) DB library.

type Database struct {
	Path       string
	connection *sql.DB
	mutex      sync.RWMutex
}

type EntryInput struct {
	Type         string
	Key          string
	Value        []byte
	Grouping     string
	SortingIndex int64
}

type DbEntry struct {
	Type         string
	Key          string
	Value        []byte
	Grouping     string
	SortingIndex int64
}

func RootPath() string {
	home, err := os.UserHomeDir()

	if err != nil {
		log.Fatal(err)
	}

	return path.Join(home, ".sidb")
}

var ErrNoDbConnection = errors.New("no database connection")

func Init(namespace []string, name string) (*Database, error) {
	dirPath := path.Join(append([]string{RootPath()}, namespace...)...)
	dbPath := path.Join(dirPath, name+".db")

	// Ensure parent directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	connection, err := sql.Open("sqlite3", dbPath)

	if err != nil {
		return nil, err
	}

	createTableSQL := `CREATE TABLE IF NOT EXISTS entries (
		"key" TEXT NOT NULL,
		"type" TEXT NOT NULL,
		"grouping" TEXT,
		"sortingIndex" INTEGER,
		"value" BLOB,
		PRIMARY KEY ("key", "type")
	) WITHOUT ROWID;

		CREATE INDEX IF NOT EXISTS idx_entries_key ON entries(type);
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
		CREATE INDEX IF NOT EXISTS idx_index_type_field_num ON entry_indexes(entry_type, field_name, field_value_num);
	`

	_, err = connection.Exec(createTableSQL)

	if err != nil {
		connection.Close()
		return nil, err
	}

	database := &Database{Path: dbPath, connection: connection, mutex: sync.RWMutex{}}

	return database, nil
}

func (db *Database) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return nil
	}

	err := db.connection.Close()
	if err != nil {
		return err
	}
	db.connection = nil
	return nil
}

func (db *Database) Get(entryType string, key string) (*DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	row := db.connection.QueryRow("SELECT type, value, key, grouping, sortingIndex FROM entries WHERE type = ? AND key = ?", entryType, key)

	var entry DbEntry
	err := row.Scan(&entry.Type, &entry.Value, &entry.Key, &entry.Grouping, &entry.SortingIndex)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No entry found
		} else {
			return nil, err
		}
	}

	return &entry, nil
}

func (db *Database) BulkGet(entryType string, keys []string) (map[string]DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	if len(keys) == 0 {
		return make(map[string]DbEntry), nil
	}
	placeholders := strings.Repeat("?,", len(keys))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma

	query := fmt.Sprintf("SELECT type, value, key, grouping, sortingIndex FROM entries WHERE key IN (%s) AND type = ?", placeholders)

	args := make([]interface{}, len(keys)+1)
	for i, key := range keys {
		args[i] = key
	}
	args[len(keys)] = entryType

	rows, err := db.connection.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make(map[string]DbEntry)

	for rows.Next() {
		var entry DbEntry
		if err := rows.Scan(&entry.Type, &entry.Value, &entry.Key, &entry.Grouping, &entry.SortingIndex); err != nil {
			return nil, err
		}
		entries[entry.Key] = entry
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (db *Database) Upsert(entry EntryInput) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("INSERT OR REPLACE INTO entries(type, value, key, grouping, sortingIndex) VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(entry.Type, entry.Value, entry.Key, entry.Grouping, entry.SortingIndex)

	return err
}

// IndexValue represents a field to be indexed
type IndexValue struct {
	FieldName string
	StrValue  *string
	NumValue  *float64
}

// upsertIndexes replaces all indexes for a given entry
func (db *Database) upsertIndexes(entryKey string, entryType string, indexes []IndexValue) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	tx, err := db.connection.Begin()
	if err != nil {
		return err
	}

	// Delete existing indexes for this entry
	_, err = tx.Exec("DELETE FROM entry_indexes WHERE entry_key = ? AND entry_type = ?", entryKey, entryType)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Insert new indexes
	if len(indexes) > 0 {
		stmt, err := tx.Prepare("INSERT INTO entry_indexes(entry_key, entry_type, field_name, field_value_str, field_value_num) VALUES(?, ?, ?, ?, ?)")
		if err != nil {
			tx.Rollback()
			return err
		}
		defer stmt.Close()

		for _, idx := range indexes {
			_, err := stmt.Exec(entryKey, entryType, idx.FieldName, idx.StrValue, idx.NumValue)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

func (db *Database) UpsertReturning(entry EntryInput) (*DbEntry, error) {
	err := db.Upsert(entry)
	if err != nil {
		return nil, err
	}

	return db.Get(entry.Type, entry.Key)
}

func (db *Database) Update(entry EntryInput) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("UPDATE entries SET value = ? WHERE key = ? AND type = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(entry.Value, entry.Key, entry.Type)

	return err
}

func (db *Database) Delete(entryType string, key string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("DELETE FROM entries WHERE key = ? AND type = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(key, entryType)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) BulkDelete(entryType string, keys []string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	if len(keys) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?,", len(keys))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma

	query := fmt.Sprintf("DELETE FROM entries WHERE key IN (%s) AND type = ?", placeholders)

	args := make([]interface{}, len(keys)+1)
	for i, key := range keys {
		args[i] = key
	}
	args[len(keys)] = entryType

	_, err := db.connection.Exec(query, args...)

	return err
}

func (db *Database) DeleteByGrouping(entryType string, grouping string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	stmt, err := db.connection.Prepare("DELETE FROM entries WHERE type = ? AND grouping = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(entryType, grouping)
	return err
}

func (db *Database) BulkUpsert(entries []EntryInput) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	tx, err := db.connection.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO entries(type, value, key, grouping, sortingIndex) VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, e := range entries {
		if _, err := stmt.Exec(e.Type, e.Value, e.Key, e.Grouping, e.SortingIndex); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (db *Database) Count() (int64, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return 0, ErrNoDbConnection
	}

	row := db.connection.QueryRow("SELECT COUNT(*) FROM entries")

	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

type SortOrder int

const (
	Ascending SortOrder = iota
	Descending
)

// FilterOp represents a filter operation
type FilterOp string

const (
	OpEqual              FilterOp = "="
	OpNotEqual           FilterOp = "!="
	OpGreaterThan        FilterOp = ">"
	OpGreaterThanOrEqual FilterOp = ">="
	OpLessThan           FilterOp = "<"
	OpLessThanOrEqual    FilterOp = "<="
	OpLike               FilterOp = "LIKE"
)

// Filter represents a filter on an indexed field
type Filter struct {
	FieldName string
	Op        FilterOp
	Value     interface{}
}

type QueryParams struct {
	From      *int64
	To        *int64
	Type      *string
	Limit     *int
	Offset    *int
	Grouping  *string
	SortOrder SortOrder
	Filters   []Filter // NEW: filters on indexed fields
}

func (db *Database) Query(
	params QueryParams,
) ([]DbEntry, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return nil, ErrNoDbConnection
	}

	var query string
	var args []interface{}

	// Build query with JOINs if filters are present
	if len(params.Filters) > 0 {
		query = "SELECT DISTINCT e.type, e.value, e.key, e.grouping, e.sortingIndex FROM entries e"

		// Add JOIN for each filter
		for i := range params.Filters {
			query += fmt.Sprintf(" INNER JOIN entry_indexes i%d ON i%d.entry_key = e.key AND i%d.entry_type = e.type", i, i, i)
		}

		query += " WHERE 1=1"

		// Add filter conditions
		for i, filter := range params.Filters {
			query += fmt.Sprintf(" AND i%d.field_name = ?", i)
			args = append(args, filter.FieldName)

			// Determine if it's a string or number filter
			_, isString := filter.Value.(string)
			if isString {
				query += fmt.Sprintf(" AND i%d.field_value_str %s ?", i, filter.Op)
			} else {
				query += fmt.Sprintf(" AND i%d.field_value_num %s ?", i, filter.Op)
			}
			args = append(args, filter.Value)
		}
	} else {
		query = "SELECT type, value, key, grouping, sortingIndex FROM entries WHERE 1=1"
	}

	if params.Type != nil {
		if len(params.Filters) > 0 {
			query += " AND e.type = ?"
		} else {
			query += " AND type = ?"
		}
		args = append(args, *params.Type)
	}

	if params.From != nil {
		if len(params.Filters) > 0 {
			query += " AND e.sortingIndex >= ?"
		} else {
			query += " AND sortingIndex >= ?"
		}
		args = append(args, *params.From)
	}

	if params.To != nil {
		if len(params.Filters) > 0 {
			query += " AND e.sortingIndex <= ?"
		} else {
			query += " AND sortingIndex <= ?"
		}
		args = append(args, *params.To)
	}

	if params.Grouping != nil {
		if len(params.Filters) > 0 {
			query += " AND e.grouping = ?"
		} else {
			query += " AND grouping = ?"
		}
		args = append(args, *params.Grouping)
	}

	order := "DESC"
	if params.SortOrder == Ascending {
		order = "ASC"
	}

	if len(params.Filters) > 0 {
		query += " ORDER BY e.sortingIndex " + order
	} else {
		query += " ORDER BY sortingIndex " + order
	}

	if params.Limit != nil {
		query += " LIMIT ?"
		args = append(args, *params.Limit)
	}

	if params.Offset != nil {
		query += " OFFSET ?"
		args = append(args, *params.Offset)
	}

	rows, err := db.connection.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []DbEntry
	for rows.Next() {
		var entry DbEntry
		if err := rows.Scan(&entry.Type, &entry.Value, &entry.Key, &entry.Grouping, &entry.SortingIndex); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (db *Database) Drop() error {
	err := db.Close()

	if err != nil {
		return err
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()
	return os.Remove(db.Path)
}

// toFloat64 converts various numeric types to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// IndexType represents the type of indexed field
type IndexType int

const (
	StringIndex IndexType = iota
	NumberIndex
)

// IndexExtractor defines how to extract an indexed field from a value
type IndexExtractor[T any] struct {
	Extract func(T) interface{}
	Type    IndexType
}

// A Store is a generic type-safe wrapper around Database for a specific entry type.

type Store[T any] struct {
	db                 *Database
	entryType          string
	serialize          func(T) ([]byte, error)
	deserialize        func([]byte) (T, error)
	deriveSortingIndex func(T) int64
	indexExtractors    map[string]IndexExtractor[T]
}

func (store *Store[T]) Get(key string) (T, error) {
	entry, err := store.db.Get(store.entryType, key)
	if err != nil || entry == nil {
		var zero T
		return zero, err
	}
	return store.deserialize(entry.Value)
}

func (store *Store[T]) BulkGet(keys []string) (map[string]T, error) {
	entries, err := store.db.BulkGet(store.entryType, keys)
	if err != nil {
		return nil, err
	}
	result := make(map[string]T)
	for key, entry := range entries {
		value, err := store.deserialize(entry.Value)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}

type StoreEntryInput[T any] struct {
	Key      string
	Value    T
	Grouping string
}

func (store *Store[T]) Upsert(entry StoreEntryInput[T]) error {
	serialized, err := store.serialize(entry.Value)
	if err != nil {
		return err
	}

	var sortingIndex int64
	if store.deriveSortingIndex != nil {
		sortingIndex = store.deriveSortingIndex(entry.Value)
	} else {
		sortingIndex = time.Now().UnixMilli()
	}

	err = store.db.Upsert(EntryInput{
		Type:         store.entryType,
		Key:          entry.Key,
		Value:        serialized,
		Grouping:     entry.Grouping,
		SortingIndex: sortingIndex,
	})
	if err != nil {
		return err
	}

	// Extract and upsert indexes
	if len(store.indexExtractors) > 0 {
		indexes := make([]IndexValue, 0, len(store.indexExtractors))
		for fieldName, extractor := range store.indexExtractors {
			value := extractor.Extract(entry.Value)
			var idx IndexValue
			idx.FieldName = fieldName

			switch extractor.Type {
			case StringIndex:
				if str, ok := value.(string); ok {
					idx.StrValue = &str
				}
			case NumberIndex:
				// Convert any numeric type to float64
				if num, ok := toFloat64(value); ok {
					idx.NumValue = &num
				}
			}

			indexes = append(indexes, idx)
		}

		if err := store.db.upsertIndexes(entry.Key, store.entryType, indexes); err != nil {
			return err
		}
	}

	return nil
}

func (store *Store[T]) Delete(key string) error {
	return store.db.Delete(store.entryType, key)
}

func (store *Store[T]) BulkDelete(keys []string) error {
	return store.db.BulkDelete(store.entryType, keys)
}

func (store *Store[T]) DeleteByGrouping(grouping string) error {
	return store.db.DeleteByGrouping(store.entryType, grouping)
}

func (store *Store[T]) BulkUpsert(entries []StoreEntryInput[T]) error {
	var dbEntries []EntryInput
	var allIndexes []struct {
		key     string
		indexes []IndexValue
	}

	for _, entry := range entries {
		serialized, err := store.serialize(entry.Value)
		if err != nil {
			return err
		}
		var sortingIndex int64
		if store.deriveSortingIndex != nil {
			sortingIndex = store.deriveSortingIndex(entry.Value)
		} else {
			sortingIndex = time.Now().UnixMilli()
		}
		dbEntries = append(dbEntries, EntryInput{
			Type:         store.entryType,
			Key:          entry.Key,
			Value:        serialized,
			Grouping:     entry.Grouping,
			SortingIndex: sortingIndex,
		})

		// Extract indexes for this entry
		if len(store.indexExtractors) > 0 {
			indexes := make([]IndexValue, 0, len(store.indexExtractors))
			for fieldName, extractor := range store.indexExtractors {
				value := extractor.Extract(entry.Value)
				var idx IndexValue
				idx.FieldName = fieldName

				switch extractor.Type {
				case StringIndex:
					if str, ok := value.(string); ok {
						idx.StrValue = &str
					}
				case NumberIndex:
					if num, ok := toFloat64(value); ok {
						idx.NumValue = &num
					}
				}

				indexes = append(indexes, idx)
			}
			allIndexes = append(allIndexes, struct {
				key     string
				indexes []IndexValue
			}{key: entry.Key, indexes: indexes})
		}
	}

	err := store.db.BulkUpsert(dbEntries)
	if err != nil {
		return err
	}

	// Upsert indexes for all entries
	for _, entryIndexes := range allIndexes {
		if err := store.db.upsertIndexes(entryIndexes.key, store.entryType, entryIndexes.indexes); err != nil {
			return err
		}
	}

	return nil
}

func (store *Store[T]) Count() (int64, error) {
	store.db.mutex.RLock()
	defer store.db.mutex.RUnlock()

	if store.db.connection == nil {
		return 0, ErrNoDbConnection
	}

	row := store.db.connection.QueryRow("SELECT COUNT(*) FROM entries WHERE type = ?", store.entryType)

	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

type StoreQueryParams struct {
	From      *int64
	To        *int64
	Limit     *int
	Offset    *int
	Grouping  *string
	SortOrder SortOrder
	Filters   []Filter
}

// QueryBuilder provides a fluent API for building queries
type QueryBuilder[T any] struct {
	store  *Store[T]
	params StoreQueryParams
}

func (store *Store[T]) Query() *QueryBuilder[T] {
	return &QueryBuilder[T]{
		store: store,
		params: StoreQueryParams{
			SortOrder: Descending,
		},
	}
}

// Where adds a filter on an indexed field
func (qb *QueryBuilder[T]) Where(fieldName string, op FilterOp, value interface{}) *QueryBuilder[T] {
	qb.params.Filters = append(qb.params.Filters, Filter{
		FieldName: fieldName,
		Op:        op,
		Value:     value,
	})
	return qb
}

// From sets the minimum sortingIndex
func (qb *QueryBuilder[T]) From(from int64) *QueryBuilder[T] {
	qb.params.From = &from
	return qb
}

// To sets the maximum sortingIndex
func (qb *QueryBuilder[T]) To(to int64) *QueryBuilder[T] {
	qb.params.To = &to
	return qb
}

// Grouping filters by grouping field
func (qb *QueryBuilder[T]) Grouping(grouping string) *QueryBuilder[T] {
	qb.params.Grouping = &grouping
	return qb
}

func (qb *QueryBuilder[T]) SortOrder(sortOrder SortOrder) *QueryBuilder[T] {
	qb.params.SortOrder = sortOrder
	return qb
}

// Limit sets the maximum number of results
func (qb *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	qb.params.Limit = &limit
	return qb
}

// Offset sets the number of results to skip
func (qb *QueryBuilder[T]) Offset(offset int) *QueryBuilder[T] {
	qb.params.Offset = &offset
	return qb
}

func (qb *QueryBuilder[T]) Exec() ([]T, error) {
	return qb.store.query(qb.params)
}

func (store *Store[T]) query(params StoreQueryParams) ([]T, error) {
	entries, err := store.db.Query(QueryParams{
		From:      params.From,
		To:        params.To,
		Type:      &store.entryType,
		Limit:     params.Limit,
		Offset:    params.Offset,
		Grouping:  params.Grouping,
		SortOrder: params.SortOrder,
		Filters:   params.Filters,
	})
	if err != nil {
		return nil, err
	}
	var results []T
	for _, entry := range entries {
		value, err := store.deserialize(entry.Value)
		if err != nil {
			return nil, err
		}
		results = append(results, value)
	}
	return results, nil
}

func (store *Store[T]) QueryEntries(params StoreQueryParams) ([]DbEntry, error) {
	return store.db.Query(QueryParams{
		From:      params.From,
		To:        params.To,
		Type:      &store.entryType,
		Limit:     params.Limit,
		Offset:    params.Offset,
		Grouping:  params.Grouping,
		SortOrder: params.SortOrder,
		Filters:   params.Filters,
	})
}

func (store *Store[T]) DropParentDb() error {
	return store.db.Drop()
}

func (store *Store[T]) GetParentDb() *Database {
	return store.db
}

func MakeStore[T any](
	db *Database,
	entryType string,
	serialize func(T) ([]byte, error),
	deserialize func([]byte) (T, error),
	deriveSortingIndex func(T) int64,
	indexExtractors map[string]IndexExtractor[T]) *Store[T] {
	if indexExtractors == nil {
		indexExtractors = make(map[string]IndexExtractor[T])
	}
	return &Store[T]{
		db:                 db,
		entryType:          entryType,
		serialize:          serialize,
		deserialize:        deserialize,
		deriveSortingIndex: deriveSortingIndex,
		indexExtractors:    indexExtractors,
	}
}
