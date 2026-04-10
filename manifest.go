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
	return m.hashes
}

func (m *Manifest) Append(hash string) {
	m.hashes = append(m.hashes, hash)
}

func (m *Manifest) Prepend(hashes []string) {
	m.hashes = append(hashes, m.hashes...)
}

func (m *Manifest) Clear() {
	m.hashes = make([]string, 0)
}

func (m *Manifest) TrimEnd(l int) {
	m.hashes = m.hashes[:len(m.hashes)-l]
}

func (m *Manifest) Save() error {
	content := strings.Join(m.hashes, "\n") + "\n"

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
	return os.Rename(tempfile.Name(), m.path)
}
