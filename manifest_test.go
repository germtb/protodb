package protodb

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aabbccdd000000000000000000000000000000000000000000000000000000aa", "11223344000000000000000000000000000000000000000000000000000000bb", "deadbeef000000000000000000000000000000000000000000000000000000cc"}
	if err := m.Update(want); err != nil {
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
	if !slices.Equal(m2.Hashes(), want) {
		t.Fatalf("after reopen: got %v, want %v", m2.Hashes(), want)
	}
}

func TestManifestMultipleSnapshotsKeepsLast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Update([]string{"aabbccdd000000000000000000000000000000000000000000000000000000aa"}); err != nil {
		t.Fatal(err)
	}
	if err := m.Update([]string{"aabbccdd000000000000000000000000000000000000000000000000000000aa", "11223344000000000000000000000000000000000000000000000000000000bb"}); err != nil {
		t.Fatal(err)
	}
	final := []string{"deadbeef000000000000000000000000000000000000000000000000000000cc"}
	if err := m.Update(final); err != nil {
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
	if !slices.Equal(m2.Hashes(), final) {
		t.Fatalf("reopen should return the last snapshot: got %v, want %v", m2.Hashes(), final)
	}
}

func TestManifestTrimEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest")

	m, err := newManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Update([]string{"aabbccdd000000000000000000000000000000000000000000000000000000aa", "11223344000000000000000000000000000000000000000000000000000000bb", "deadbeef000000000000000000000000000000000000000000000000000000cc"}); err != nil {
		t.Fatal(err)
	}
	if err := m.TrimEnd(2); err != nil {
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
	want := []string{"aabbccdd000000000000000000000000000000000000000000000000000000aa"}
	if !slices.Equal(m2.Hashes(), want) {
		t.Fatalf("after TrimEnd+reopen: got %v, want %v", m2.Hashes(), want)
	}
}
