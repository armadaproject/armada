//go:build unix

package mpibootstrap

import "syscall"

// CapOpenFiles lowers the soft limit on open files of this process, and of the processes it starts, to
// Config.MaxOpenFiles. It does nothing if the limit is already that low, or if MaxOpenFiles is zero.
func (c Config) CapOpenFiles() error {
	if c.MaxOpenFiles <= 0 {
		return nil
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return err
	}
	if limit.Cur <= uint64(c.MaxOpenFiles) {
		return nil
	}
	limit.Cur = uint64(c.MaxOpenFiles)
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit)
}
