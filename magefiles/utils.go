package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/pkg/errors"
)

func binaryWithExt(name string) string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("%s.exe", name)
	}
	return name
}

func checkOs() string {
	if runtime.GOOS == "windows" {
		return "windows"
	}
	return runtime.GOOS
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
	return runtime.GOARCH == "arm64"
}

// Validates that arg is one of validArgs.
// Returns nil if arg is valid, error otherwise.
func validateArg(arg string, validArgs []string) error {
	valid := false
	for _, validArg := range validArgs {
		if arg == validArg {
			valid = true
			break
		}
	}

	if !valid {
		return errors.Errorf("invalid argument: %s, expected one of: %s", arg, validArgs)
	}
	return nil
}
