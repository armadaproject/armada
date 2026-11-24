//go:build unix

// armada-mpi starts the runtime daemons of an MPI job on the workers of an Armada MPI job without ssh.
// Armada adds it to the pods of an MPI job; see package mpibootstrap for how it works.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/armadaproject/armada/internal/mpibootstrap"
)

const usage = `armada-mpi starts MPI runtime daemons on the workers of an Armada MPI job without ssh.

Usage:
  armada-mpi launch -- <command...>   run in the launcher: wait for the workers, then run the command
  armada-mpi worker                   run in a worker: stand by for commands from the launcher
  armada-mpi agent <host> <command>   called by mpirun in place of ssh
  armada-mpi install <directory>      copy this program into a directory shared with the other containers
`

func main() {
	os.Exit(run(os.Args))
}

func run(args []string) int {
	// MPI launchers that take the path of a remote shell, but no arguments for it, call armada-mpi-agent.
	if filepath.Base(args[0]) == "armada-mpi-agent" {
		args = append([]string{args[0], "agent"}, args[1:]...)
	}
	if len(args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	config, err := mpibootstrap.ConfigFromEnv()
	if err != nil {
		return fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if args[1] == "launch" || args[1] == "worker" {
		if err := config.CapOpenFiles(); err != nil {
			fmt.Fprintf(os.Stderr, "armada-mpi: cannot lower the limit on open files: %s\n", err)
		}
	}

	switch args[1] {
	case "launch":
		command := args[2:]
		if len(command) > 0 && command[0] == "--" {
			command = command[1:]
		}
		// Signals are passed on to the command instead of stopping the launcher.
		stop()
		exitCode, err := mpibootstrap.Launch(context.Background(), config, command, net.DefaultResolver.LookupHost, os.Stderr)
		if err != nil {
			fail(err)
		}
		return exitCode
	case "worker":
		hostname, err := mpibootstrap.WorkerHostnameFromOS()
		if err != nil {
			return fail(err)
		}
		_ = mpibootstrap.Work(ctx, config, hostname, os.Stderr)
		return 0
	case "agent":
		exitCode, err := mpibootstrap.Agent(config, args[2:], os.Stderr)
		if err != nil {
			fail(err)
		}
		return exitCode
	case "install":
		if len(args) != 3 {
			fmt.Fprint(os.Stderr, usage)
			return 2
		}
		if err := mpibootstrap.Install(args[2]); err != nil {
			return fail(err)
		}
		return 0
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
}

func fail(err error) int {
	fmt.Fprintf(os.Stderr, "armada-mpi: %s\n", err)
	return 1
}
