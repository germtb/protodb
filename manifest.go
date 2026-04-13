package protodb

import (
	"os"
	"path/filepath"
	"strings"
)

type Manifest struct {
	path   string
	hashes []string
}

func newManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{path: path}, nil
		}
		return nil, err
	}

	content := strings.TrimSpace(string(data))
	var hashes []string
	if content != "" {
		hashes = strings.Split(content, "\n")
	}

	return &Manifest{path: path, hashes: hashes}, nil
}

func (m *Manifest) Hashes() []string {
	return m.hashes[:]
}

func (m *Manifest) TrimEnd(l int) error {
	new_hashes := m.hashes[:len(m.hashes)-l]
	return m.Update(new_hashes)
}

func (m *Manifest) Update(hashes []string) error {
	content := strings.Join(hashes, "\n") + "\n"

	dir := filepath.Dir(m.path)
	tempfile, err := os.CreateTemp(dir, ".manifest-temp-")
	if err != nil {
		return err
	}

	if _, err := tempfile.WriteString(content); err != nil {
		tempfile.Close()
		os.Remove(tempfile.Name())
		return err
	}

	if err := tempfile.Sync(); err != nil {
		tempfile.Close()
		os.Remove(tempfile.Name())
		return err
	}

	tempfile.Close()
	err = os.Rename(tempfile.Name(), m.path)
	if err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	m.hashes = hashes
	return nil
}

// syncDir fsyncs a directory so a preceding Rename is durable. Without it,
// a crash right after Rename can leave the directory entry unpersisted and
// the renamed file disappears on reboot.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return err
	}
	return dir.Close()
}
