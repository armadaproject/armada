package fileutils

import (
	"errors"
	"io/fs"
	"os"
)

// IsFileOrDirectory returns true if path points to an existing file or directory
// and false otherwise. Errors not satisfying errors.Is(err, fs.ErrNotExist) are
// passed on to the caller.
func IsFileOrDirectory(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// IsFile returns true if path points to an existing file
// and false otherwise. Errors not satisfying errors.Is(err, fs.ErrNotExist) are
// passed on to the caller.
func IsFile(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err == nil {
		if fileInfo.IsDir() {
			return false, nil
		}
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// IsDirectory returns true if path points to an existing directory
// and false otherwise. Errors not satisfying errors.Is(err, fs.ErrNotExist) are
// passed on to the caller.
func IsDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err == nil {
		if fileInfo.IsDir() {
			return true, nil
		}
		return false, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}
