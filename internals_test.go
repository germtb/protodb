package protodb

import (
	"bytes"
	"os"
	"testing"
)

// Micro-benchmarks to isolate []byte key overhead

func BenchmarkMemtablePut(b *testing.B) {
	m := newMemtable()
	k := key(42)
	v := []byte("value")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		m.Put(k, v, uint64(iter+1))
	}
}

func BenchmarkSkiplistPut(b *testing.B) {
	sl := NewSkiplist()
	k := key(42)
	v := []byte("value")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		sl.Put(k, v, uint64(iter+1))
	}
}

func BenchmarkSkiplistPutSequential(b *testing.B) {
	sl := NewSkiplist()
	v := []byte("value")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		sl.Put(key(uint64(iter)), v, uint64(iter+1))
	}
}

func BenchmarkMemtablePutSequential(b *testing.B) {
	m := newMemtable()
	v := []byte("value")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		m.Put(key(uint64(iter)), v, uint64(iter+1))
	}
}

func BenchmarkSkiplistPut100K(b *testing.B) {
	v := make([]byte, 200)
	for iter := 0; iter < b.N; iter++ {
		sl := NewSkiplist()
		for i := 0; i < 100_000; i++ {
			sl.Put(key(uint64(i)), v, uint64(i+1))
		}
	}
}

func BenchmarkMemtablePut100K(b *testing.B) {
	v := make([]byte, 200)
	for iter := 0; iter < b.N; iter++ {
		m := newMemtable()
		for i := 0; i < 100_000; i++ {
			m.Put(key(uint64(i)), v, uint64(i+1))
		}
	}
}

func BenchmarkSkiplistGet(b *testing.B) {
	sl := NewSkiplist()
	for idx := uint64(0); idx < 10000; idx++ {
		sl.Put(key(idx), []byte("value"), uint64(idx+1))
	}
	k := key(5000)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		sl.Get(k, VisibleAll)
	}
}

func BenchmarkMemtableGet(b *testing.B) {
	m := newMemtable()
	for idx := uint64(0); idx < 10000; idx++ {
		m.Put(key(idx), []byte("value"), uint64(idx+1))
	}
	k := key(5000)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		m.Get(k, VisibleAll)
	}
}

func BenchmarkMemtableGetWithKeyAlloc(b *testing.B) {
	m := newMemtable()
	for idx := uint64(0); idx < 10000; idx++ {
		m.Put(key(idx), []byte("value"), uint64(idx+1))
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		m.Get(key(uint64(iter)%10000), VisibleAll)
	}
}

func BenchmarkWriteFrame(b *testing.B) {
	k := key(42)
	v := []byte("some value here")
	var buf bytes.Buffer
	buf.Grow(1024)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		buf.Reset()
		writeFrame(&buf, k, v)
	}
}

func BenchmarkWriteFrameWithKeyAlloc(b *testing.B) {
	v := []byte("some value here")
	var buf bytes.Buffer
	buf.Grow(1024)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		buf.Reset()
		writeFrame(&buf, key(uint64(iter)), v)
	}
}

func BenchmarkBytesCompare(b *testing.B) {
	a := key(12345)
	c := key(12346)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		bytes.Compare(a, c)
	}
}

// Direct SST iteration with no merge wrapper.
func BenchmarkSSTIterate1000(b *testing.B) {
	dir := b.TempDir()
	pairs := make([]KeyValue, 10000)
	for idx := range pairs {
		pairs[idx].Key = key(uint64(idx))
		pairs[idx].Value = []byte("value")
	}
	ssts, err := WriteSST(dir, iter(pairs), true)
	if err != nil {
		b.Fatal(err)
	}
	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		b.Fatal(err)
	}
	f, _ := openFile(s)
	defer f.Close()

	lo := key(0)
	hi := key(1000)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		count := 0
		it := s.Iterator(lo, hi, f, false)
		for it.Next() {
			count++
		}
	}
}

func BenchmarkSSTGet(b *testing.B) {
	dir := b.TempDir()
	pairs := make([]KeyValue, 10000)
	for idx := range pairs {
		pairs[idx].Key = key(uint64(idx))
		pairs[idx].Value = []byte("value")
	}
	ssts, err := WriteSST(dir, iter(pairs), true)
	if err != nil {
		b.Fatal(err)
	}
	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		b.Fatal(err)
	}
	f, err := openFile(s)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	k := key(5000)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		s.Get(k, f)
	}
}

func BenchmarkSSTGetWithKeyAlloc(b *testing.B) {
	dir := b.TempDir()
	pairs := make([]KeyValue, 10000)
	for idx := range pairs {
		pairs[idx].Key = key(uint64(idx))
		pairs[idx].Value = []byte("value")
	}
	ssts, err := WriteSST(dir, iter(pairs), true)
	if err != nil {
		b.Fatal(err)
	}
	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		b.Fatal(err)
	}
	f, err := openFile(s)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		s.Get(key(uint64(iter)%10000), f)
	}
}

func openFile(s *sst) (*os.File, error) {
	return os.Open(s.path)
}

// BenchmarkEnginePut measures end-to-end write throughput, including WAL
// append and commit-loop overhead. No flush in the loop.
func BenchmarkEnginePut(b *testing.B) {
	engine, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	v := []byte("value")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		engine.Put(key(uint64(iter)), v)
	}
}

// BenchmarkEngineGetMemtable measures Get against a 10k-key memtable (no SSTs).
func BenchmarkEngineGetMemtable(b *testing.B) {
	engine, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	for idx := uint64(0); idx < 10000; idx++ {
		engine.Put(key(idx), []byte("value"))
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		engine.Get(key(uint64(iter) % 10000))
	}
}

// BenchmarkEnginePutSameKey hammers a single key — worst case for the
// append-only skiplist since every version accumulates until flush.
func BenchmarkEnginePutSameKey(b *testing.B) {
	engine, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	k := key(42)
	v := []byte("value")
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		engine.Put(k, v)
	}
}
