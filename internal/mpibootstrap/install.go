//go:build unix

package mpibootstrap

import (
	"io"
	"os"
	"path/filepath"
)

// Install copies the running program into dir, as armada-mpi and as armada-mpi-agent.
// An init container runs it to put the program on a volume shared with the containers of the user, whose image
// does not contain it.
func Install(dir string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	source, err := os.Open(self)
	if err != nil {
		return err
	}
	defer source.Close()
	target := filepath.Join(dir, BinaryName)
	destination, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(destination, source); err != nil {
		destination.Close()
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	agent := filepath.Join(dir, AgentBinaryName)
	_ = os.Remove(agent)
	return os.Symlink(BinaryName, agent)
}
