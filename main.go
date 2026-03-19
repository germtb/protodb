package protodb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

const maxSQLPlaceholders = 500

var ErrNoDbConnection = errors.New("no database connection")
var ErrInvalidFilterOp = errors.New("invalid filter operator")

// filterOpSQL maps each FilterOp constant to its trusted SQL string.
// Only operators in this map are allowed — this prevents SQL injection
// via arbitrary FilterOp values.
var filterOpSQL = map[FilterOp]string{
	OpEqual:              "=",
	OpNotEqual:           "!=",
	OpGreaterThan:        ">",
	OpGreaterThanOrEqual: ">=",
	OpLessThan:           "<",
	OpLessThanOrEqual:    "<=",
	OpLike:               "LIKE",
}

func Init(dbPath string) (*Database, error) {
	connection, err := sql.Open("sqlite3", dbPath)

	if err != nil {
		return nil, err
	}

	if _, err := connection.Exec("PRAGMA foreign_keys = ON"); err != nil {
		connection.Close()
		return nil, err
	}

	if _, err := connection.Exec("PRAGMA journal_mode=WAL"); err != nil {
		connection.Close()
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
		CREATE INDEX IF NOT EXISTS idx_index_type_field_num ON entry_indexes(entry_type, field_name, field_value_num);
	`

	_, err = connection.Exec(createTableSQL)

	if err != nil {
		connection.Close()
		return nil, err
	}

	database := &Database{
		Path:       dbPath,
		connection: connection,
		mutex:      sync.RWMutex{},
	}

	return database, nil
}

// DatabaseExists checks if a database file exists without creating it
func DatabaseExists(dbPath string) bool {
	_, err := os.Stat(dbPath)
	return err == nil
}

func (db *Database) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return nil
	}

	err := db.connection.Close()
	db.connection = nil
	return err
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

	entries := make(map[string]DbEntry)

	for start := 0; start < len(keys); start += maxSQLPlaceholders {
		end := start + maxSQLPlaceholders
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]

		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = placeholders[:len(placeholders)-1]

		query := fmt.Sprintf("SELECT type, value, key, grouping, sortingIndex FROM entries WHERE key IN (%s) AND type = ?", placeholders)

		args := make([]interface{}, len(chunk)+1)
		for i, key := range chunk {
			args[i] = key
		}
		args[len(chunk)] = entryType

		rows, err := db.connection.Query(query, args...)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var entry DbEntry
			if err := rows.Scan(&entry.Type, &entry.Value, &entry.Key, &entry.Grouping, &entry.SortingIndex); err != nil {
				rows.Close()
				return nil, err
			}
			entries[entry.Key] = entry
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	return entries, nil
}

func (db *Database) Upsert(entry EntryInput) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	_, err := db.connection.Exec("INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?, ?, ?, ?, ?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex",
		entry.Type, entry.Value, entry.Key, entry.Grouping, entry.SortingIndex)
	return err
}

// IndexValue represents a field to be indexed
type IndexValue struct {
	FieldName string
	StrValue  *string
	NumValue  *float64
}

// upsertWithIndexes performs an entry upsert and index upsert atomically
// in a single transaction under one lock.
func (db *Database) upsertWithIndexes(entry EntryInput, indexes []IndexValue) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	tx, err := db.connection.Begin()
	if err != nil {
		return err
	}

	// Upsert the entry
	_, err = tx.Exec("INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?, ?, ?, ?, ?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex",
		entry.Type, entry.Value, entry.Key, entry.Grouping, entry.SortingIndex)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Delete existing indexes for this entry
	_, err = tx.Exec("DELETE FROM entry_indexes WHERE entry_key = ? AND entry_type = ?", entry.Key, entry.Type)
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
			_, err := stmt.Exec(entry.Key, entry.Type, idx.FieldName, idx.StrValue, idx.NumValue)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

// bulkUpsertWithIndexes performs bulk entry upserts and their index upserts atomically
// in a single transaction under one lock.
func (db *Database) bulkUpsertWithIndexes(entries []EntryInput, allIndexes [][]IndexValue) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	if len(entries) == 0 {
		return nil
	}

	tx, err := db.connection.Begin()
	if err != nil {
		return err
	}

	entryStmt, err := tx.Prepare("INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?, ?, ?, ?, ?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer entryStmt.Close()

	var idxStmt *sql.Stmt
	hasIndexes := len(allIndexes) > 0
	if hasIndexes {
		idxStmt, err = tx.Prepare("INSERT INTO entry_indexes(entry_key, entry_type, field_name, field_value_str, field_value_num) VALUES(?, ?, ?, ?, ?)")
		if err != nil {
			tx.Rollback()
			return err
		}
		defer idxStmt.Close()
	}

	for i, e := range entries {
		if _, err := entryStmt.Exec(e.Type, e.Value, e.Key, e.Grouping, e.SortingIndex); err != nil {
			tx.Rollback()
			return err
		}

		if hasIndexes && i < len(allIndexes) && len(allIndexes[i]) > 0 {
			// Delete existing indexes for this entry
			_, err = tx.Exec("DELETE FROM entry_indexes WHERE entry_key = ? AND entry_type = ?", e.Key, e.Type)
			if err != nil {
				tx.Rollback()
				return err
			}

			for _, idx := range allIndexes[i] {
				_, err := idxStmt.Exec(e.Key, e.Type, idx.FieldName, idx.StrValue, idx.NumValue)
				if err != nil {
					tx.Rollback()
					return err
				}
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

func (db *Database) Delete(entryType string, key string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	_, err := db.connection.Exec("DELETE FROM entries WHERE key = ? AND type = ?", key, entryType)
	return err
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

	tx, err := db.connection.Begin()
	if err != nil {
		return err
	}

	for start := 0; start < len(keys); start += maxSQLPlaceholders {
		end := start + maxSQLPlaceholders
		if end > len(keys) {
			end = len(keys)
		}
		chunk := keys[start:end]

		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = placeholders[:len(placeholders)-1]

		query := fmt.Sprintf("DELETE FROM entries WHERE key IN (%s) AND type = ?", placeholders)

		args := make([]interface{}, len(chunk)+1)
		for i, key := range chunk {
			args[i] = key
		}
		args[len(chunk)] = entryType

		if _, err := tx.Exec(query, args...); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (db *Database) DeleteByGrouping(entryType string, grouping string) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection == nil {
		return ErrNoDbConnection
	}

	_, err := db.connection.Exec("DELETE FROM entries WHERE type = ? AND grouping = ?", entryType, grouping)
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

	stmt, err := tx.Prepare("INSERT INTO entries(type, value, key, grouping, sortingIndex) VALUES(?, ?, ?, ?, ?) ON CONFLICT(key, type) DO UPDATE SET value=excluded.value, grouping=excluded.grouping, sortingIndex=excluded.sortingIndex")
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
	Value     any
}

type QueryParams struct {
	From      *int64
	To        *int64
	Type      *string
	Limit     *int
	Offset    *int
	Grouping  *string
	SortOrder SortOrder
	Filters   []Filter
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

	query = "SELECT DISTINCT e.type, e.value, e.key, e.grouping, e.sortingIndex FROM entries e"

	// Add JOIN for each filter
	for i := range params.Filters {
		query += fmt.Sprintf(" INNER JOIN entry_indexes i%d ON i%d.entry_key = e.key AND i%d.entry_type = e.type", i, i, i)
	}

	query += " WHERE 1=1"

	// Add filter conditions
	for i, filter := range params.Filters {
		sqlOp, ok := filterOpSQL[filter.Op]
		if !ok {
			return nil, ErrInvalidFilterOp
		}
		query += fmt.Sprintf(" AND i%d.field_name = ?", i)
		args = append(args, filter.FieldName)

		// Determine if it's a string or number filter
		_, isString := filter.Value.(string)
		if isString {
			query += fmt.Sprintf(" AND i%d.field_value_str %s ?", i, sqlOp)
		} else {
			query += fmt.Sprintf(" AND i%d.field_value_num %s ?", i, sqlOp)
		}
		args = append(args, filter.Value)
	}

	if params.Type != nil {
		query += " AND e.type = ?"
		args = append(args, *params.Type)
	}

	if params.From != nil {
		query += " AND e.sortingIndex >= ?"
		args = append(args, *params.From)
	}

	if params.To != nil {
		query += " AND e.sortingIndex <= ?"
		args = append(args, *params.To)
	}

	if params.Grouping != nil {
		query += " AND e.grouping = ?"
		args = append(args, *params.Grouping)
	}

	order := "DESC"
	if params.SortOrder == Ascending {
		order = "ASC"
	}

	query += " ORDER BY e.sortingIndex " + order

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
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.connection != nil {
		db.connection.Close()
		db.connection = nil
	}
	os.Remove(db.Path + "-wal")
	os.Remove(db.Path + "-shm")
	return os.Remove(db.Path)
}

// A Store is a generic type-safe wrapper around Database for a specific entry type.
// T must be a proto.Message (pointer to a protobuf-generated type).

type Store[T proto.Message] struct {
	db                 *Database
	entryType          string
	indexFields        []string
	fieldDescCache     map[string]protoreflect.FieldDescriptor
	newT               func() T
	deriveSortingIndex func(T) int64
}

// MakeStore creates a new protobuf-native Store.
// T must be a pointer to a protobuf-generated type (e.g. *userpb.User).
// The entry type is derived from the proto message's full name.
// deriveSortingIndex optionally provides a custom sorting index (pass nil to use current time).
// indexFields optionally specify proto field names to create secondary indexes on.
func MakeStore[T proto.Message](db *Database, deriveSortingIndex func(T) int64, indexFields ...string) *Store[T] {
	var zero T
	elemType := reflect.TypeOf(zero).Elem()
	newT := func() T {
		return reflect.New(elemType).Interface().(T)
	}

	entryType := string(newT().ProtoReflect().Descriptor().FullName())

	s := &Store[T]{
		db:                 db,
		entryType:          entryType,
		newT:               newT,
		indexFields:        indexFields,
		deriveSortingIndex: deriveSortingIndex,
	}

	if len(indexFields) > 0 {
		s.fieldDescCache = make(map[string]protoreflect.FieldDescriptor, len(indexFields))
		msg := newT()
		md := msg.ProtoReflect().Descriptor()
		fields := md.Fields()
		for _, fieldName := range indexFields {
			fd := fields.ByName(protoreflect.Name(fieldName))
			if fd == nil {
				panic(fmt.Sprintf("sidb: proto type %s has no field %q", md.FullName(), fieldName))
			}
			switch fd.Kind() {
			case protoreflect.StringKind,
				protoreflect.Int32Kind, protoreflect.Int64Kind,
				protoreflect.Sint32Kind, protoreflect.Sint64Kind,
				protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind,
				protoreflect.Uint32Kind, protoreflect.Uint64Kind,
				protoreflect.Fixed32Kind, protoreflect.Fixed64Kind,
				protoreflect.FloatKind, protoreflect.DoubleKind,
				protoreflect.BoolKind, protoreflect.EnumKind:
				// supported
			default:
				panic(fmt.Sprintf("sidb: field %q of proto type %s has unsupported kind %s for indexing", fieldName, md.FullName(), fd.Kind()))
			}
			s.fieldDescCache[fieldName] = fd
		}
	}

	return s
}

func (store *Store[T]) Get(key string) (T, error) {
	entry, err := store.db.Get(store.entryType, key)
	if err != nil {
		var zero T
		return zero, err
	}
	if entry == nil {
		var zero T
		return zero, nil
	}
	msg := store.newT()
	if err := proto.Unmarshal(entry.Value, msg); err != nil {
		var zero T
		return zero, err
	}
	return msg, nil
}

func (store *Store[T]) BulkGet(keys []string) (map[string]T, error) {
	entries, err := store.db.BulkGet(store.entryType, keys)
	if err != nil {
		return nil, err
	}
	result := make(map[string]T, len(entries))
	for key, entry := range entries {
		msg := store.newT()
		if err := proto.Unmarshal(entry.Value, msg); err != nil {
			return nil, err
		}
		result[key] = msg
	}
	return result, nil
}

type StoreEntryInput[T proto.Message] struct {
	Key      string
	Value    T
	Grouping string
}

func (store *Store[T]) Upsert(entry StoreEntryInput[T]) error {
	serialized, err := proto.Marshal(entry.Value)
	if err != nil {
		return err
	}

	var sortingIndex int64
	if store.deriveSortingIndex != nil {
		sortingIndex = store.deriveSortingIndex(entry.Value)
	} else {
		sortingIndex = time.Now().UnixMilli()
	}

	dbEntry := EntryInput{
		Type:         store.entryType,
		Key:          entry.Key,
		Value:        serialized,
		Grouping:     entry.Grouping,
		SortingIndex: sortingIndex,
	}

	if len(store.indexFields) > 0 {
		indexes := store.extractIndexes(entry.Value)
		return store.db.upsertWithIndexes(dbEntry, indexes)
	}
	return store.db.Upsert(dbEntry)
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

func (store *Store[T]) extractIndexes(value T) []IndexValue {
	msg := value.ProtoReflect()
	indexes := make([]IndexValue, 0, len(store.indexFields))
	for _, fieldName := range store.indexFields {
		fd := store.fieldDescCache[fieldName]
		val := msg.Get(fd)
		var idx IndexValue
		idx.FieldName = fieldName

		switch fd.Kind() {
		case protoreflect.StringKind:
			s := val.String()
			idx.StrValue = &s
		case protoreflect.Int32Kind, protoreflect.Int64Kind,
			protoreflect.Sint32Kind, protoreflect.Sint64Kind,
			protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
			n := float64(val.Int())
			idx.NumValue = &n
		case protoreflect.Uint32Kind, protoreflect.Uint64Kind,
			protoreflect.Fixed32Kind, protoreflect.Fixed64Kind:
			n := float64(val.Uint())
			idx.NumValue = &n
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			n := val.Float()
			idx.NumValue = &n
		case protoreflect.BoolKind:
			var n float64
			if val.Bool() {
				n = 1.0
			}
			idx.NumValue = &n
		case protoreflect.EnumKind:
			n := float64(val.Enum())
			idx.NumValue = &n
		}

		indexes = append(indexes, idx)
	}
	return indexes
}

func (store *Store[T]) BulkUpsert(entries []StoreEntryInput[T]) error {
	var dbEntries []EntryInput
	var allIndexes [][]IndexValue
	hasIndexes := len(store.indexFields) > 0

	for _, entry := range entries {
		serialized, err := proto.Marshal(entry.Value)
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

		if hasIndexes {
			allIndexes = append(allIndexes, store.extractIndexes(entry.Value))
		}
	}

	if hasIndexes {
		return store.db.bulkUpsertWithIndexes(dbEntries, allIndexes)
	}
	return store.db.BulkUpsert(dbEntries)
}

func (db *Database) CountByType(entryType string) (int64, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	if db.connection == nil {
		return 0, ErrNoDbConnection
	}

	row := db.connection.QueryRow("SELECT COUNT(*) FROM entries WHERE type = ?", entryType)

	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (store *Store[T]) Count() (int64, error) {
	return store.db.CountByType(store.entryType)
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
type QueryBuilder[T proto.Message] struct {
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
func (qb *QueryBuilder[T]) Where(fieldName string, op FilterOp, value any) *QueryBuilder[T] {
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
		msg := store.newT()
		if err := proto.Unmarshal(entry.Value, msg); err != nil {
			return nil, err
		}
		results = append(results, msg)
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
