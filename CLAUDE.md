# CLAUDE.md - AI Assistant Guide for ProtoDB

## Project Overview

**ProtoDB** is a protobuf-native embedded key-value database for Go, backed by SQLite. It provides a clean abstraction layer for storing and retrieving protobuf messages with rich querying capabilities.

**Core Value Proposition:**

- Protobuf-native: stores proto.Message types with zero serialization boilerplate
- Type-safe operations through Go generics constrained to `proto.Message`
- Thread-safe concurrent access
- Rich querying with secondary indexes, filtering, sorting, and pagination
- Zero configuration - databases auto-created at specified root paths

## Architecture

### Core Components

1. **Database** (`main.go`)

   - Low-level SQLite wrapper with raw CRUD operations
   - Thread-safe with `sync.RWMutex` for concurrent reads
   - Manages connection lifecycle and transactions

2. **Store[T proto.Message]** (`main.go`)

   - Generic type-safe wrapper around Database, constrained to `proto.Message`
   - Uses `proto.Marshal`/`proto.Unmarshal` for serialization
   - Automatic secondary index extraction via protobuf reflection (`protoreflect`)
   - Supports custom sorting index derivation via function
   - Options pattern: `NewStore[T](db, entryType, IndexFields[T]("field1"), WithSortingIndexFunc[T](fn))`

### Data Model

- **EntryInput**: Input for create/update operations (Type, Key, Value, Grouping, SortingIndex)
- **DbEntry**: Retrieved entry
- **Schema**: Composite key (type + key), indexed on type, grouping, sortingIndex
- **Secondary Indexes**: `entry_indexes` table with foreign key cascading deletes

### Key Design Patterns

**Thread Safety:**

- Read operations use `db.mutex.RLock()` for concurrent reads
- Write operations use `db.mutex.Lock()` for exclusive access
- Always defer unlock immediately after lock

**Atomic Operations:**

- `upsertWithIndexes` and `bulkUpsertWithIndexes` perform entry + index writes in a single transaction under one lock

**Protobuf Serialization:**

- `Store[T]` requires `T` to be a `proto.Message` (always a pointer type like `*userpb.User`)
- `proto.Marshal`/`proto.Unmarshal` for serialization
- Secondary index values extracted via `protoreflect.FieldDescriptor` with kind-based branching
- New instances created via `reflect.New` on the element type

## Working with the Codebase

### Proto Definitions

- **`internal/testpb/test.proto`**: TestItem, TestUser (test-only)

### Adding New Features

**When adding database operations:**

1. Add low-level method to `Database` struct with proper locking
2. Add corresponding typed method to `Store[T]` if applicable
3. Write comprehensive tests in `main_test.go`
4. Consider transaction support for bulk operations

**When adding new proto index types:**

1. Add kind handling in `Store.extractIndexes()` (switch on `protoreflect.Kind`)
2. Map to either `IndexValue.StrValue` or `IndexValue.NumValue`

### Testing Approach

Tests use temporary directories (cleaned up with `defer db.Drop()` or `t.TempDir()`):

- **`main_test.go`**: Core functionality tests using proto types from `internal/testpb`

**Test Structure Pattern:**

```go
func TestFeature(t *testing.T) {
    root := t.TempDir()
    db, _ := Init(root, []string{"ns"}, "test-feature")
    defer db.Drop()
    // ... test operations
}
```

## Quick Reference

**Opening a database:**

```go
db, err := protodb.Init(root, []string{"namespace"}, "dbname")
defer db.Close()
```

**Type-safe operations with protobuf:**

```go
store := protodb.NewStore[*userpb.User](db, "users",
    protodb.IndexFields[*userpb.User]("email", "age"),
)
store.Upsert(protodb.StoreEntryInput[*userpb.User]{
    Key:   "user1",
    Value: &userpb.User{Name: "Alice", Email: "alice@example.com", Age: 30},
})
user, _ := store.Get("user1") // returns *userpb.User
```

**Querying with filters:**

```go
results, _ := store.Query().
    Where("age", protodb.OpGreaterThan, 25).
    Where("email", protodb.OpLike, "%@example.com").
    Limit(10).
    Exec()
```

## File Reference

- **main.go**: Core library (Database, Store, all operations, schema DDL)
- **main_test.go**: Core test suite
- **internal/testpb/test.proto**: Test protobuf definitions
- **go.mod**: Module definition (`github.com/germtb/protodb`)
