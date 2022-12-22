package main

import (
	"fmt"
	"runtime"
)

func binaryWithExt(name string) string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf("%s.exe", name)
	}
	return name
}
