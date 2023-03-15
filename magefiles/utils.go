package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

func binaryWithExt(name string) string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("%s.exe", name)
	}
	return name
}

func copy(srcPath, dstPath string) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), os.ModeDir|0o755); err != nil {
		return err
	}
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}

func set[S ~[]E, E comparable](s S) map[E]bool {
	m := make(map[E]bool)
	for _, v := range s {
		m[v] = true
	}
	return m
}

// Check if the user is on an arm system
func onArm() bool {
	return runtime.GOARCH == "arm"
}
