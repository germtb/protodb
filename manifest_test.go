package protodb

import (
	"encoding/hex"
	"path/filepath"
	"reflect"
	"testing"
)

func mustHash(s string) sstHash {
	var h sstHash
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		panic(err)
	}
	return h
}

var (
	hashA = mustHash("aabbccdd000000000000000000000000000000000000000000000000000000aa")
	hashB = mustHash("11223344000000000000000000000000000000000000000000000000000000bb")
	hashC = mustHash("deadbeef000000000000000000000000000000000000000000000000000000cc")
)

func metaA() LevelMetadata { return LevelMetadata{hash: hashA, first: Key("aa"), last: Key("az")} }
func metaB() LevelMetadata { return LevelMetadata{hash: hashB, first: Key("ba"), last: Key("bz")} }
func metaC() LevelMetadata { return LevelMetadata{hash: hashC, first: Key("ca"), last: Key("cz")} }

func TestManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(DefaultFS, path)
	if err != nil {
		t.Fatal(err)
	}
	wantL0 := []LevelMetadata{metaA(), metaB()}
	wantL1 := []LevelMetadata{metaC()}
	if err := m.Update(levelL0, wantL0); err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL1, wantL1); err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
	m.handle.Close()

	m2, err := newManifest(DefaultFS, path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m2.L0(), wantL0) {
		t.Fatalf("L0: got %v, want %v", m2.L0(), wantL0)
	}
	if !reflect.DeepEqual(m2.L1(), wantL1) {
		t.Fatalf("L1: got %v, want %v", m2.L1(), wantL1)
	}
}

func TestManifestLastFrameWins(t *testing.T) {
	// Successive L0 frames should each fully replace the previous L0 state.
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(DefaultFS, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL0, []LevelMetadata{metaA()}); err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL0, []LevelMetadata{metaA(), metaB()}); err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL0, []LevelMetadata{metaC()}); err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
	m.handle.Close()

	m2, err := newManifest(DefaultFS, path)
	if err != nil {
		t.Fatal(err)
	}
	want := []LevelMetadata{metaC()}
	if !reflect.DeepEqual(m2.L0(), want) {
		t.Fatalf("reopen should return the last L0 frame: got %v, want %v", m2.L0(), want)
	}
	if len(m2.L1()) != 0 {
		t.Fatalf("L1 should be empty, got %v", m2.L1())
	}
}

func TestManifestIndependentLevels(t *testing.T) {
	// L0 updates must not affect L1 and vice versa.
	path := filepath.Join(t.TempDir(), "manifest")

	m, _ := newManifest(DefaultFS, path)
	m.Update(levelL1, []LevelMetadata{metaA()})
	m.Update(levelL0, []LevelMetadata{metaB()})
	m.Update(levelL0, []LevelMetadata{metaC()})
	m.Sync()
	m.handle.Close()

	m2, _ := newManifest(DefaultFS, path)
	if !reflect.DeepEqual(m2.L0(), []LevelMetadata{metaC()}) {
		t.Errorf("L0: got %v", m2.L0())
	}
	if !reflect.DeepEqual(m2.L1(), []LevelMetadata{metaA()}) {
		t.Errorf("L1: got %v", m2.L1())
	}
}
