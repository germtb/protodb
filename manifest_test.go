package protodb

import (
	"path/filepath"
	"slices"
	"testing"
)

const hashA = "aabbccdd000000000000000000000000000000000000000000000000000000aa"
const hashB = "11223344000000000000000000000000000000000000000000000000000000bb"
const hashC = "deadbeef000000000000000000000000000000000000000000000000000000cc"

func TestManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	wantL0 := []string{hashA, hashB}
	wantL1 := []string{hashC}
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

	m2, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(m2.L0Hashes(), wantL0) {
		t.Fatalf("L0: got %v, want %v", m2.L0Hashes(), wantL0)
	}
	if !slices.Equal(m2.L1Hashes(), wantL1) {
		t.Fatalf("L1: got %v, want %v", m2.L1Hashes(), wantL1)
	}
}

func TestManifestLastFrameWins(t *testing.T) {
	// Successive L0 frames should each fully replace the previous L0 state.
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL0, []string{hashA}); err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL0, []string{hashA, hashB}); err != nil {
		t.Fatal(err)
	}
	if err := m.Update(levelL0, []string{hashC}); err != nil {
		t.Fatal(err)
	}
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
	m.handle.Close()

	m2, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{hashC}
	if !slices.Equal(m2.L0Hashes(), want) {
		t.Fatalf("reopen should return the last L0 frame: got %v, want %v", m2.L0Hashes(), want)
	}
	if len(m2.L1Hashes()) != 0 {
		t.Fatalf("L1 should be empty, got %v", m2.L1Hashes())
	}
}

func TestManifestIndependentLevels(t *testing.T) {
	// L0 updates must not affect L1 and vice versa.
	path := filepath.Join(t.TempDir(), "manifest")

	m, _ := newManifest(path)
	m.Update(levelL1, []string{hashA})
	m.Update(levelL0, []string{hashB})
	m.Update(levelL0, []string{hashC})
	m.Sync()
	m.handle.Close()

	m2, _ := newManifest(path)
	if !slices.Equal(m2.L0Hashes(), []string{hashC}) {
		t.Errorf("L0: got %v", m2.L0Hashes())
	}
	if !slices.Equal(m2.L1Hashes(), []string{hashA}) {
		t.Errorf("L1: got %v", m2.L1Hashes())
	}
}
