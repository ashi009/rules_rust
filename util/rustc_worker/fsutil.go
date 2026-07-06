package main

import (
	"io"
	"os"
	"path/filepath"
)

// atomicFile is a temp file that lands at dst when Closed (standard io.Closer
// semantics: Close commits). Discard is defer-safe — a no-op after Close.
type atomicFile struct {
	f      *os.File
	dst    string
	closed bool
}

func newAtomicFile(dst string) (*atomicFile, error) {
	dir, base := filepath.Split(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, base+".tmp*")
	if err != nil {
		return nil, err
	}
	return &atomicFile{f: f, dst: dst}, nil
}

func (a *atomicFile) Write(p []byte) (int, error) { return a.f.Write(p) }

func (a *atomicFile) Close() error {
	if err := a.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(a.f.Name(), a.dst); err != nil {
		return err
	}
	a.closed = true
	return nil
}

func (a *atomicFile) Discard() {
	if a.closed {
		return
	}
	a.f.Close()
	os.Remove(a.f.Name())
}

func atomicWrite(dst string, content []byte) error {
	f, err := newAtomicFile(dst)
	if err != nil {
		return err
	}
	defer f.Discard()
	if _, err := f.Write(content); err != nil {
		return err
	}
	return f.Close()
}

func atomicCopy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	f, err := newAtomicFile(dst)
	if err != nil {
		return err
	}
	defer f.Discard()
	if _, err := io.Copy(f, in); err != nil {
		return err
	}
	return f.Close()
}

// move is mv(1): rename when possible, streaming copy across filesystems.
func move(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	os.Remove(dst)
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	return atomicCopy(src, dst)
}
