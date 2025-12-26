# CLAUDE.md - AI Assistant Guide for SIDB

## Project Overview

**SIDB** (Si(mple) DB) is a lightweight, type-safe key-value database library for Go, backed by SQLite. It provides a clean abstraction layer for storing and retrieving typed data with rich querying capabilities.

**Core Value Proposition:**

- Simple, intuitive API with minimal boilerplate
- Type-safe operations through Go generics
- Thread-safe concurrent access
- Rich querying with filtering, sorting, and pagination
- Zero configuration - databases auto-created in `~/.sidb/{namespace}/{name}.db`

## Architecture

### Core Components

1. **Database** (`main.go:44-72`)

   - Low-level SQLite wrapper with raw CRUD operations
   - Thread-safe with `sync.RWMutex` for concurrent reads
   - Manages connection lifecycle and transactions

2. **Store[T]** (`main.go:283-301`)

   - Generic type-safe wrapper around Database
   - Handles serialization/deserialization using `encoding/gob`
   - Provides typed CRUD operations for any serializable Go type
   - Supports custom sorting index derivation via function

3. **Data Model**
   - **EntryInput** (`main.go:26-33`): Input for create/update operations
   - **DbEntry** (`main.go:35-42`): Retrieved entry with computed timestamp
   - **Schema**: Composite key (type + key), indexed on type, grouping, sortingIndex, timestamp

### Key Design Patterns

**Thread Safety:**

- Read operations use `db.mu.RLock()` for concurrent reads
- Write operations use `db.mu.Lock()` for exclusive access
- Always defer unlock immediately after lock

**Transactions:**

- Bulk operations use transactions for atomicity and performance
- Pattern: `BEGIN -> operations -> COMMIT/ROLLBACK`
- See `BulkUpsert`, `BulkDelete` for examples

**Type Safety:**

- Use `Store[T]` for type-safe operations on custom types
- Generic constraints: any serializable type works with `encoding/gob`
- Example: `Store[MyStruct]` provides `Get() (*MyStruct, error)` instead of `Get() (*DbEntry, error)`

**Error Handling:**

- Database errors are propagated up
- "Not found" returns `(nil, nil)` in Soft operations
- "Not found" returns error in regular Get operations

## Working with the Codebase

### Adding New Features

**When adding database operations:**

1. Add low-level method to `Database` struct
2. Use proper locking (RLock for reads, Lock for writes)
3. Add corresponding typed method to `Store[T]` if applicable
4. Write comprehensive tests in `main_test.go`
5. Consider transaction support for bulk operations

**When modifying schema:**

- Schema is defined in `initDB()` (`main.go:74-107`)
- Indexes are critical for query performance
- Current indexes: type, grouping, sortingIndex, timestamp
- WITHOUT ROWID optimization requires PRIMARY KEY to be first in CREATE TABLE

### Testing Approach

Tests are in `main_test.go` and use:

- Temporary test namespaces (cleaned up with `defer db.Drop()`)
- Table-driven tests for comprehensive coverage
- Real SQLite database (not mocked) for integration testing

**Test Structure Pattern:**

```go
func TestFeature(t *testing.T) {
    db, _ := Open("test-namespace", "test-feature")
    defer db.Drop()
    // ... test operations
}
```

### Common Tasks

**Adding a new query filter:**

1. Add field to `QueryParams` struct (`main.go:212-222`)
2. Update `Query()` method (`main.go:347-427`) to build WHERE clause
3. Add test case in `main_test.go`

**Adding a new operation:**

1. Implement on `Database` with proper locking
2. Add generic wrapper to `Store[T]`
3. Write tests covering edge cases
4. Update README.md with usage example

**Performance optimization:**

- Check query plans with EXPLAIN QUERY PLAN
- Consider adding indexes for new query patterns
- Use transactions for bulk operations
- Profile with Go's pprof if needed

## Important Patterns and Conventions

### Naming Conventions

- **Type**: Category/table name for grouping entries (e.g., "users", "posts")
- **Key**: Unique identifier within a type (e.g., user ID, post ID)
- **Grouping**: Optional field for batch operations (e.g., "session-123", "batch-2024")
- **SortingIndex**: Custom ordering field (e.g., display order, priority)

### Serialization

- Uses `encoding/gob` for type serialization
- Value stored as BLOB in database
- Custom types must be gob-compatible (exported fields, no channels/functions)
- Register complex types with `gob.Register()` if needed

### Database Location

- Default: `~/.sidb/{namespace}/{name}.db`
- Namespace provides isolation between different apps/environments
- Example: `Open("myapp", "production")` → `~/.sidb/myapp/production.db`

### Sorting Index Derivation

`Store[T]` supports custom sorting via `WithSortingIndexFunc`:

```go
store := NewStoreWithSortingIndex[MyType](db, "mytype",
    func(t *MyType) int64 {
        return t.Priority // custom sort field
    })
```

## Gotchas and Considerations

### Thread Safety

- `Database` is thread-safe, but individual operations are not transactions
- Multiple operations requiring consistency should be wrapped in explicit transactions
- Future consideration: Add BeginTx/CommitTx/RollbackTx methods for user transactions

### Timestamps

- Default: current time on upsert
- Can override with custom timestamp for data migration/import
- Stored as Unix timestamp (int64)

### Primary Key Behavior

- Composite key: (key, type)
- UPSERT semantics: INSERT OR REPLACE
- No automatic ID generation - caller provides keys

### Performance Notes

- WITHOUT ROWID optimization used for better performance
- Bulk operations use transactions (10-100x faster than individual inserts)
- Query with appropriate filters and indexes for best performance
- Consider LIMIT/OFFSET for pagination on large datasets

## File Reference

- **main.go**: Core library implementation (Database, Store, all operations)
- **main_test.go**: Comprehensive test suite
- **README.md**: User-facing documentation
- **TODO.md**: Planned features and improvements
- **go.mod**: Module definition (`github.com/gerdooshell/sidb`)

## Quick Reference

**Opening a database:**

```go
db, err := sidb.Open("namespace", "dbname")
defer db.Close()
```

**Type-safe operations:**

```go
store := sidb.NewStore[MyType](db, "mytype")
store.Upsert("key1", &myData)
data, _ := store.Get("key1")
```

**Querying:**

```go
entries, _ := store.Query(sidb.QueryParams{
    Type: "users",
    GroupingFilter: &grouping,
    SortField: sidb.SortByTimestamp,
    SortOrder: sidb.Descending,
    Limit: 10,
})
```

**Bulk operations:**

```go
store.BulkUpsert([]sidb.EntryInput{...})
store.DeleteByGrouping("session-123")
```
