package fileutils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsFileOrDirectory(t *testing.T) {
	dir := t.TempDir()

	// existing directory
	path := dir
	exists, err := IsFileOrDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != true {
		t.Fatalf("expected true, but got false")
	}

	// non-existing directory
	path = filepath.Join(dir, "kjdsfjkfhd")
	exists, err = IsFileOrDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}

	// existing file
	file, err := os.CreateTemp(dir, "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	file.Close()

	path = file.Name()
	exists, err = IsFileOrDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != true {
		t.Fatalf("expected true, but got false")
	}

	// non-existing file
	path = filepath.Join(dir, "skdfsdf3")
	exists, err = IsFileOrDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}
}

func TestIsFile(t *testing.T) {
	dir := t.TempDir()

	// existing directory
	path := dir
	exists, err := IsFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}

	// non-existing directory
	path = filepath.Join(dir, "kjdsfjkfhd")
	exists, err = IsFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}

	// existing file
	file, err := os.CreateTemp(dir, "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	file.Close()

	path = file.Name()
	exists, err = IsFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != true {
		t.Fatalf("expected true, but got false")
	}

	// non-existing file
	path = filepath.Join(dir, "skdfsdf3")
	exists, err = IsFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}
}

func TestIsDirectory(t *testing.T) {
	dir := t.TempDir()

	// existing directory
	path := dir
	exists, err := IsDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != true {
		t.Fatalf("expected true, but got false")
	}

	// non-existing directory
	path = filepath.Join(dir, "kjdsfjkfhd")
	exists, err = IsDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}

	// existing file
	file, err := os.CreateTemp(dir, "")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	file.Close()

	path = file.Name()
	exists, err = IsDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}

	// non-existing file
	path = filepath.Join(dir, "skdfsdf3")
	exists, err = IsDirectory(path)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if exists != false {
		t.Fatalf("expected false, but got true")
	}
}
