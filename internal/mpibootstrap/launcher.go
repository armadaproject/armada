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
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Resolver looks up the addresses of a host. It is net.DefaultResolver.LookupHost outside of tests.
type Resolver func(ctx context.Context, host string) ([]string, error)

// launcher keeps the connections of the workers that have called home and are waiting for a command.
type launcher struct {
	config   Config
	resolver Resolver
	logs     io.Writer

	mux sync.Mutex
	// idle holds, by worker hostname, the connection of each worker waiting for a command.
	idle map[string]*channel
	// changed is closed and replaced whenever idle changes, to wake up everything waiting on it.
	changed chan struct{}
	// seen records the workers that have called home at least once.
	seen map[string]bool
}

func newLauncher(config Config, resolver Resolver, logs io.Writer) *launcher {
	return &launcher{
		config:   config,
		resolver: resolver,
		logs:     logs,
		idle:     make(map[string]*channel),
		changed:  make(chan struct{}),
		seen:     make(map[string]bool),
	}
}

// Launch runs command, the command of the user that starts the MPI job, with the workers of the job standing by.
// It writes the hostfiles, waits until every worker has called home, runs the command and returns its exit code.
// While the command runs, it serves the requests Agent makes on behalf of the MPI launcher.
func Launch(ctx context.Context, config Config, command []string, resolver Resolver, logs io.Writer) (int, error) {
	if len(command) == 0 {
		return 1, errors.New("usage: armada-mpi launch -- <command...>")
	}
	if config.Workers < 1 {
		return 1, fmt.Errorf("%s must be at least 1", WorkersEnvVar)
	}
	if err := config.writeHostfiles(); err != nil {
		return 1, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	l := newLauncher(config, resolver, logs)
	stop, err := l.start(ctx)
	if err != nil {
		return 1, err
	}
	defer stop()

	fmt.Fprintf(logs, "armada-mpi: waiting for %d workers to call home\n", config.Workers)
	if err := l.waitForWorkers(ctx); err != nil {
		return 1, err
	}
	fmt.Fprintf(logs, "armada-mpi: all workers are standing by, running: %v\n", command)
	exitCode, err := run(ctx, command)
	if err == nil && exitCode == 0 {
		l.dismissWorkers(ctx)
	}
	return exitCode, err
}

// dismissWorkers ends the workers once the job has succeeded, so that the whole job ends successfully rather than
// with workers that Armada had to stop. It tells every worker that it is done and waits for the worker to hang up,
// which the worker does as it ends.
//
// It then waits for Config.SettleDelay before returning, upon which the launcher ends. Armada stops whatever is
// left of the gang when the launcher succeeds, so it must learn that the workers have ended before it learns that
// the launcher has. Nothing here can observe what Armada knows; the delay keeps the two ends apart by more than it
// takes a kubelet to report that a container has ended. If the order is reversed all the same, a worker that has
// in fact ended is recorded as cancelled rather than as succeeded, which is how every worker ended before workers
// were dismissed.
//
// A worker that has not hung up after Config.DismissTimeout is left to be stopped by Armada. A job that failed is
// not handled here at all: its workers are stopped by Armada too.
func (l *launcher) dismissWorkers(ctx context.Context) {
	if l.config.DismissTimeout <= 0 {
		return
	}
	fmt.Fprintf(l.logs, "armada-mpi: the command succeeded, dismissing the workers\n")
	dismissCtx, cancel := context.WithTimeout(ctx, l.config.DismissTimeout)
	defer cancel()
	var wg sync.WaitGroup
	var remaining atomic.Int32
	for i := 0; i < l.config.Workers; i++ {
		hostname := WorkerHostname(i)
		remaining.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.dismissWorker(dismissCtx, hostname) {
				remaining.Add(-1)
			}
		}()
	}
	wg.Wait()
	if left := remaining.Load(); left > 0 {
		fmt.Fprintf(l.logs, "armada-mpi: %d workers had not ended after %s\n", left, l.config.DismissTimeout)
		return
	}
	fmt.Fprintf(l.logs, "armada-mpi: all workers have ended\n")
	select {
	case <-ctx.Done():
	case <-time.After(l.config.SettleDelay):
	}
}

// dismissWorker dismisses one worker and reports whether the worker hung up before ctx was done.
func (l *launcher) dismissWorker(ctx context.Context, hostname string) bool {
	worker, err := l.take(ctx, hostname)
	if err != nil {
		return false
	}
	defer worker.close()
	stop := context.AfterFunc(ctx, worker.close)
	defer stop()
	if err := worker.send(message{Dismissed: true}); err != nil {
		return false
	}
	// The worker sends nothing more: the read returns when the worker hangs up, or when ctx is done.
	_, _ = worker.receive()
	return ctx.Err() == nil
}

// start begins accepting the connections of the workers and the requests of the agent.
// The returned function stops accepting them.
func (l *launcher) start(ctx context.Context) (func(), error) {
	workers, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(l.config.Port)))
	if err != nil {
		return nil, err
	}
	_ = os.Remove(l.config.agentSocket())
	agents, err := net.Listen("unix", l.config.agentSocket())
	if err != nil {
		_ = workers.Close()
		return nil, err
	}
	go serve(workers, l.handleWorker)
	go serve(agents, func(conn net.Conn) { l.handleAgent(ctx, conn) })
	return func() {
		_ = workers.Close()
		_ = agents.Close()
	}, nil
}

func serve(listener net.Listener, handle func(net.Conn)) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go handle(conn)
	}
}

// handleWorker registers a worker that calls home, after checking that it is who it says it is.
func (l *launcher) handleWorker(conn net.Conn) {
	link := newChannel(conn)
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	hello, err := link.receive()
	if err != nil {
		link.close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if err := l.verifyWorker(hello.Hostname, conn.RemoteAddr()); err != nil {
		fmt.Fprintf(l.logs, "armada-mpi: refused a connection from %s: %s\n", conn.RemoteAddr(), err)
		_ = link.send(message{Error: err.Error()})
		link.close()
		return
	}

	l.mux.Lock()
	defer l.mux.Unlock()
	if previous, ok := l.idle[hello.Hostname]; ok {
		// A worker only calls home again after losing its previous connection.
		previous.close()
	}
	l.idle[hello.Hostname] = link
	l.seen[hello.Hostname] = true
	l.notify()
}

// verifyWorker checks that hostname is a worker of this job and that the connection comes from the address
// Kubernetes publishes for that worker.
func (l *launcher) verifyWorker(hostname string, remote net.Addr) error {
	if !l.isWorker(hostname) {
		return fmt.Errorf("%q is not a worker of this job", hostname)
	}
	remoteHost, _, err := net.SplitHostPort(remote.String())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addresses, err := l.resolver(ctx, l.config.address(hostname))
	if err != nil {
		return fmt.Errorf("cannot look up %s: %w", hostname, err)
	}
	remoteIP := net.ParseIP(remoteHost)
	if !slices.ContainsFunc(addresses, func(address string) bool { return net.ParseIP(address).Equal(remoteIP) }) {
		return fmt.Errorf("%s is not the address of %s", remoteHost, hostname)
	}
	return nil
}

// isWorker reports whether hostname is the hostname of one of the workers of this job.
func (l *launcher) isWorker(hostname string) bool {
	suffix, ok := strings.CutPrefix(hostname, WorkerHostnamePrefix)
	if !ok {
		return false
	}
	index, err := strconv.Atoi(suffix)
	return err == nil && index >= 0 && index < l.config.Workers && hostname == WorkerHostname(index)
}

// notify wakes up everything waiting for idle to change. The caller must hold mux.
func (l *launcher) notify() {
	close(l.changed)
	l.changed = make(chan struct{})
}

func (l *launcher) waitForWorkers(ctx context.Context) error {
	deadline := time.After(l.config.ReadyTimeout)
	for {
		l.mux.Lock()
		seen, changed := len(l.seen), l.changed
		l.mux.Unlock()
		if seen >= l.config.Workers {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("only %d of %d workers called home within %s", seen, l.config.Workers, l.config.ReadyTimeout)
		}
	}
}

// take removes and returns the connection of the worker once it is standing by.
func (l *launcher) take(ctx context.Context, hostname string) (*channel, error) {
	for {
		l.mux.Lock()
		link, ok := l.idle[hostname]
		if ok {
			delete(l.idle, hostname)
		}
		changed := l.changed
		l.mux.Unlock()
		if ok {
			return link, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// handleAgent serves one request of Agent: it runs the requested command on the requested worker and relays the
// output and the exit code of the command back to the agent.
func (l *launcher) handleAgent(ctx context.Context, conn net.Conn) {
	agent := newChannel(conn)
	defer agent.close()
	request, err := agent.receive()
	if err != nil {
		return
	}
	if !l.isWorker(request.Hostname) {
		_ = agent.send(message{Error: fmt.Sprintf("%q is not a worker of this job", request.Hostname)})
		return
	}

	// If the MPI launcher gives up and kills the agent, stop waiting for, or drop, the worker.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_, _ = agent.receive()
		cancel()
	}()

	worker, err := l.take(ctx, request.Hostname)
	if err != nil {
		return
	}
	// Closing the connection tells the worker to stop the command, and makes it call home again.
	defer worker.close()
	stop := context.AfterFunc(ctx, worker.close)
	defer stop()
	if err := worker.send(message{Command: request.Command}); err != nil {
		_ = agent.send(message{Error: fmt.Sprintf("lost %s: %s", request.Hostname, err)})
		return
	}
	for {
		reply, err := worker.receive()
		if err != nil {
			_ = agent.send(message{Error: fmt.Sprintf("lost %s: %s", request.Hostname, err)})
			return
		}
		if err := agent.send(reply); err != nil || reply.Exited {
			return
		}
	}
}

// run runs the command of the user, passing on termination signals, and returns its exit code.
func run(ctx context.Context, command []string) (int, error) {
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return 127, err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case sig := <-signals:
				_ = cmd.Process.Signal(sig)
			case <-ctx.Done():
				_ = cmd.Process.Signal(syscall.SIGTERM)
				return
			case <-done:
				return
			}
		}
	}()
	err := cmd.Wait()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		if exitError.ExitCode() >= 0 {
			return exitError.ExitCode(), nil
		}
		return 1, nil
	}
	if err != nil {
		return 1, err
	}
	return 0, nil
}
