//go:build linux || darwin

package replay

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func hostUsage() (cpuUS int64, peakRSSBytes uint64) {
	var usage unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &usage) != nil {
		return 0, 0
	}
	cpuUS = usage.Utime.Nano()/1000 + usage.Stime.Nano()/1000
	peakRSSBytes = uint64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		peakRSSBytes *= 1024
	}
	return cpuUS, peakRSSBytes
}
