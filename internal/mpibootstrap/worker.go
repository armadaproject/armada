//go:build unix

package mpibootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

const workerRedialInterval = time.Second

// Work is the main loop of a worker. It calls home to the launcher, runs the command the launcher sends, reports
// the outcome and calls home again, until ctx is cancelled. A worker never exits on its own: an MPI job may run
// mpirun several times, and Armada stops the workers once the launcher has finished.
func Work(ctx context.Context, config Config, hostname string, logs io.Writer) error {
	launcherHost := config.address(LauncherHostname)
	if config.launcherHost != "" {
		launcherHost = config.launcherHost
	}
	launcher := net.JoinHostPort(launcherHost, strconv.Itoa(config.Port))
	fmt.Fprintf(logs, "armada-mpi: worker %s calling home to %s\n", hostname, launcher)
	for ctx.Err() == nil {
		if err := workOnce(ctx, launcher, hostname, logs); err != nil && ctx.Err() == nil {
			// The launcher may not be up yet, or may be between two runs of mpirun: keep calling.
			select {
			case <-ctx.Done():
			case <-time.After(workerRedialInterval):
			}
		}
	}
	return ctx.Err()
}

// workOnce serves a single command: it returns once the command has ended or the connection is lost.
func workOnce(ctx context.Context, launcher, hostname string, logs io.Writer) error {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", launcher)
	if err != nil {
		return err
	}
	link := newChannel(conn)
	defer link.close()
	stop := context.AfterFunc(ctx, link.close)
	defer stop()

	if err := link.send(message{Hostname: hostname}); err != nil {
		return err
	}
	request, err := link.receive()
	if err != nil {
		return err
	}
	if request.Error != "" {
		fmt.Fprintf(logs, "armada-mpi: launcher refused worker %s: %s\n", hostname, request.Error)
		return errors.New(request.Error)
	}
	fmt.Fprintf(logs, "armada-mpi: running: %s\n", request.Command)

	// The command runs in its own process group, so that the daemon and everything it started can be stopped
	// together if the launcher goes away.
	commandCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	command := exec.CommandContext(commandCtx, "sh", "-c", request.Command)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	output := io.MultiWriter(logs, outputWriter{channel: link})
	command.Stdout = output
	command.Stderr = output
	command.Stdin = nil

	// The launcher sends nothing more: a read only returns when the connection is lost, in which case the
	// command has no one left to serve.
	go func() {
		_, _ = link.receive()
		cancel()
	}()

	exitCode := 0
	if err := command.Run(); err != nil {
		exitCode = 1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) && exitError.ExitCode() >= 0 {
			exitCode = exitError.ExitCode()
		}
	}
	fmt.Fprintf(logs, "armada-mpi: command exited with code %d\n", exitCode)
	return link.send(message{Exited: true, ExitCode: exitCode})
}

// WorkerHostnameFromOS returns the hostname of this pod, which Armada sets to the name of the worker.
func WorkerHostnameFromOS() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return shortHostname(hostname), nil
}
