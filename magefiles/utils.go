package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

func binaryWithExt(name string) string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("%s.exe", name)
	}
	return name
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func unzip(zipPath, dstPath string) error {
	read, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer read.Close()
	for _, file := range read.File {
		if strings.Contains(file.Name, "..") {
			return errors.Errorf("filename %s contains illegal '..' element (zip slip sanitation)", file.Name)
		}
		name := path.Join(dstPath, file.Name)
		if err := os.MkdirAll(path.Dir(name), os.ModeDir|0o755); err != nil {
			return err
		}
		if file.Mode().IsRegular() {
			open, err := file.Open()
			if err != nil {
				return err
			}
			defer open.Close()
			data, err := io.ReadAll(open)
			if err != nil {
				return err
			}
			if err := os.WriteFile(name, data, 0o755); err != nil {
				return err
			}
		}
	}
	return nil
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

func unique[S ~[]E, E comparable](s S) S {
	m := make(map[E]bool)
	for _, v := range s {
		m[v] = true
	}
	return maps.Keys(m)
}

func set[S ~[]E, E comparable](s S) map[E]bool {
	m := make(map[E]bool)
	for _, v := range s {
		m[v] = true
	}
	return m
}
