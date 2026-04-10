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
		m.Put(k, v)
	}
}

func BenchmarkMemtableGet(b *testing.B) {
	m := newMemtable()
	for idx := uint64(0); idx < 10000; idx++ {
		m.Put(key(idx), []byte("value"))
	}
	k := key(5000)
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		m.Get(k)
	}
}

func BenchmarkMemtableGetWithKeyAlloc(b *testing.B) {
	m := newMemtable()
	for idx := uint64(0); idx < 10000; idx++ {
		m.Put(key(idx), []byte("value"))
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		m.Get(key(uint64(iter) % 10000))
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
	pairs := make([]struct {
		key   Key
		value []byte
	}, 10000)
	for idx := range pairs {
		pairs[idx].key = key(uint64(idx))
		pairs[idx].value = []byte("value")
	}
	ssts, err := WriteSST(dir, entriesFrom(pairs))
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
		it := s.Iterator(lo, hi, f)
		for it.Next() {
			count++
		}
	}
}

func BenchmarkSSTGet(b *testing.B) {
	dir := b.TempDir()
	pairs := make([]struct {
		key   Key
		value []byte
	}, 10000)
	for idx := range pairs {
		pairs[idx].key = key(uint64(idx))
		pairs[idx].value = []byte("value")
	}
	ssts, err := WriteSST(dir, entriesFrom(pairs))
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
	pairs := make([]struct {
		key   Key
		value []byte
	}, 10000)
	for idx := range pairs {
		pairs[idx].key = key(uint64(idx))
		pairs[idx].value = []byte("value")
	}
	ssts, err := WriteSST(dir, entriesFrom(pairs))
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
