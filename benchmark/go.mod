module github.com/germtb/protodb/benchmark

go 1.24

require (
	github.com/cockroachdb/pebble v1.1.5
	github.com/germtb/protodb v0.0.0
	github.com/mattn/go-sqlite3 v1.14.32
	go.etcd.io/bbolt v1.4.3
	google.golang.org/protobuf v1.36.11
)

replace github.com/germtb/protodb => ../
