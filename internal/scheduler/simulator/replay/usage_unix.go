//go:build linux || darwin

package replay

import (
	"runtime"
	"syscall"
)

func hostUsage() (cpuUS int64, peakRSSBytes uint64) {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0, 0
	}
	cpuUS = usage.Utime.Nano()/1000 + usage.Stime.Nano()/1000
	peakRSSBytes = uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		peakRSSBytes *= 1024
	}
	return cpuUS, peakRSSBytes
}
