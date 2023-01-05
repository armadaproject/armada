package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
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
		name := path.Join(dstPath, file.Name)
		if file.Mode().IsDir() {
			os.MkdirAll(path.Dir(name), os.ModeDir|0o755)
			continue
		}
		open, err := file.Open()
		if err != nil {
			return err
		}
		defer open.Close()
		os.MkdirAll(path.Dir(name), os.ModeDir|0o755)
		create, err := os.Create(name)
		if err != nil {
			return err
		}
		defer create.Close()
		_, err = io.Copy(create, open)
		if err != nil {
			return err
		}
	}
	return nil
}

func copy(srcPath, dstPath string) error {
	err := os.MkdirAll(filepath.Dir(dstPath), os.ModeDir|0o755)
	if err != nil {
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
