//go:build !linux && !darwin

package replay

func hostUsage() (cpuUS int64, peakRSSBytes uint64) { return 0, 0 }
