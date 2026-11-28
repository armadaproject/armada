//go:build unix

package mpibootstrap

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

// Agent is what an MPI launcher calls in place of ssh: "agent [options] <host> <command...>".
// It asks the launcher process started by Launch to run the command on the worker, relays the output of the
// command to stderr, and returns the exit code of the command.
func Agent(config Config, args []string, stderr io.Writer) (int, error) {
	// Launchers may pass ssh options before the host, e.g., -x. None of them means anything here.
	for len(args) > 0 && strings.HasPrefix(args[0], "-") {
		args = args[1:]
	}
	if len(args) < 2 {
		return 1, errors.New("usage: armada-mpi agent [options] <host> <command...>")
	}
	conn, err := net.Dial("unix", config.agentSocket())
	if err != nil {
		return 1, fmt.Errorf("cannot reach the launcher: is this the launcher of an Armada MPI job? %w", err)
	}
	link := newChannel(conn)
	defer link.close()

	// As with ssh, the words of the command are joined and interpreted by a shell on the worker.
	request := message{Hostname: shortHostname(args[0]), Command: strings.Join(args[1:], " ")}
	if err := link.send(request); err != nil {
		return 1, err
	}
	for {
		reply, err := link.receive()
		if errors.Is(err, io.EOF) {
			// The launcher only goes away when the job is over, at which point there is nothing left to report.
			return 1, nil
		}
		if err != nil {
			return 1, fmt.Errorf("lost the launcher while running a command on %s: %w", request.Hostname, err)
		}
		switch {
		case reply.Error != "":
			return 1, errors.New(reply.Error)
		case reply.Exited:
			return reply.ExitCode, nil
		default:
			_, _ = stderr.Write(reply.Output)
		}
	}
}
