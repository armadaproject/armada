//go:build unix

package mpibootstrap

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockedBuffer is a bytes.Buffer that may be written to from several goroutines.
type lockedBuffer struct {
	mux    sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.buffer.String()
}

func localResolver(_ context.Context, _ string) ([]string, error) {
	return []string{"127.0.0.1"}, nil
}

func testConfig(t *testing.T, workers int) Config {
	// Reserve a free port for the launcher.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return Config{
		Workers:        workers,
		SlotsPerWorker: 2,
		Port:           port,
		Dir:            t.TempDir(),
		ServiceName:    "armada-gang-abc",
		ReadyTimeout:   10 * time.Second,
		launcherHost:   "127.0.0.1",
	}
}

// startJob starts a launcher and its workers, and waits until all workers are standing by.
func startJob(t *testing.T, config Config) *launcher {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	l := newLauncher(config, localResolver, io.Discard)
	stop, err := l.start(ctx)
	require.NoError(t, err)
	t.Cleanup(stop)
	for i := 0; i < config.Workers; i++ {
		go func() { _ = Work(ctx, config, WorkerHostname(i), io.Discard) }()
	}
	require.NoError(t, l.waitForWorkers(ctx))
	return l
}

func TestAgent_RunsCommandsOnWorkers(t *testing.T) {
	config := testConfig(t, 2)
	startJob(t, config)

	output := &lockedBuffer{}
	// MPI launchers may refer to a worker by its address and may pass ssh options before it.
	exitCode, err := Agent(config, []string{"-x", "worker-1.armada-gang-abc", "echo", "from the worker;", "exit", "3"}, output)
	require.NoError(t, err)
	assert.Equal(t, 3, exitCode)
	assert.Equal(t, "from the worker\n", output.String())

	// A worker calls home again after a command, so it can serve the next run of mpirun.
	for _, worker := range []string{"worker-0", "worker-1"} {
		exitCode, err = Agent(config, []string{worker, "true"}, io.Discard)
		require.NoError(t, err)
		assert.Equal(t, 0, exitCode)
	}
}

func TestAgent_RejectsHostsThatAreNotWorkers(t *testing.T) {
	config := testConfig(t, 2)
	startJob(t, config)

	for _, host := range []string{"worker-2", "launcher", "worker-x", "example.com"} {
		exitCode, err := Agent(config, []string{host, "true"}, io.Discard)
		assert.Error(t, err, host)
		assert.Equal(t, 1, exitCode, host)
	}
}

func TestAgent_FailsWithoutLauncher(t *testing.T) {
	_, err := Agent(testConfig(t, 1), []string{"worker-0", "true"}, io.Discard)
	assert.Error(t, err)
	_, err = Agent(testConfig(t, 1), []string{"worker-0"}, io.Discard)
	assert.Error(t, err)
}

func TestVerifyWorker(t *testing.T) {
	config := testConfig(t, 2)
	var lookedUp []string
	resolver := func(_ context.Context, host string) ([]string, error) {
		lookedUp = append(lookedUp, host)
		return []string{"10.0.0.7"}, nil
	}
	l := newLauncher(config, resolver, io.Discard)
	fromWorker := &net.TCPAddr{IP: net.ParseIP("10.0.0.7"), Port: 4000}
	fromElsewhere := &net.TCPAddr{IP: net.ParseIP("10.0.0.8"), Port: 4000}

	assert.NoError(t, l.verifyWorker("worker-1", fromWorker))
	assert.Equal(t, []string{"worker-1.armada-gang-abc"}, lookedUp)

	assert.Error(t, l.verifyWorker("worker-1", fromElsewhere), "the address must be the one published for the worker")
	for _, hostname := range []string{"worker-2", "launcher", "", "worker-", "worker-01", "worker--1"} {
		assert.Error(t, l.verifyWorker(hostname, fromWorker), hostname)
	}
}

func TestWaitForWorkers_TimesOut(t *testing.T) {
	config := testConfig(t, 1)
	config.ReadyTimeout = 50 * time.Millisecond
	l := newLauncher(config, localResolver, io.Discard)

	err := l.waitForWorkers(context.Background())

	assert.ErrorContains(t, err, "only 0 of 1 workers called home")
}

func TestLaunch_RunsCommandOnceWorkersStandBy(t *testing.T) {
	config := testConfig(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = Work(ctx, config, WorkerHostname(0), io.Discard) }()

	exitCode, err := Launch(ctx, config, []string{"sh", "-c", "exit 5"}, localResolver, io.Discard)

	require.NoError(t, err)
	assert.Equal(t, 5, exitCode)
	openMpi, err := os.ReadFile(filepath.Join(config.Dir, OpenMpiHostfileName))
	require.NoError(t, err)
	assert.Equal(t, "worker-0.armada-gang-abc slots=2\n", string(openMpi))
	hydra, err := os.ReadFile(filepath.Join(config.Dir, HydraHostfileName))
	require.NoError(t, err)
	assert.Equal(t, "worker-0.armada-gang-abc:2\n", string(hydra))
}

func TestLaunch_RequiresCommandAndWorkers(t *testing.T) {
	_, err := Launch(context.Background(), testConfig(t, 1), nil, localResolver, io.Discard)
	assert.Error(t, err)
	_, err = Launch(context.Background(), testConfig(t, 0), []string{"true"}, localResolver, io.Discard)
	assert.Error(t, err)
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(WorkersEnvVar, "4")
	t.Setenv(SlotsPerWorkerEnvVar, "8")
	t.Setenv(GangServiceNameEnvVar, "armada-gang-abc")
	t.Setenv(ReadyTimeoutEnvVar, "30s")

	config, err := ConfigFromEnv()

	require.NoError(t, err)
	assert.Equal(t, 4, config.Workers)
	assert.Equal(t, 8, config.SlotsPerWorker)
	assert.Equal(t, DefaultPort, config.Port)
	assert.Equal(t, DefaultDir, config.Dir)
	assert.Equal(t, 30*time.Second, config.ReadyTimeout)
	assert.Equal(t, "worker-3.armada-gang-abc", config.address(WorkerHostname(3)))

	t.Setenv(WorkersEnvVar, "many")
	_, err = ConfigFromEnv()
	assert.Error(t, err)
}

func TestCapOpenFiles(t *testing.T) {
	var before syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &before))
	t.Cleanup(func() { _ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &before) })

	// Zero, and a cap above the current limit, leave the limit alone.
	require.NoError(t, Config{}.CapOpenFiles())
	require.NoError(t, Config{MaxOpenFiles: int(before.Cur) + 1}.CapOpenFiles())
	var after syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &after))
	assert.Equal(t, before.Cur, after.Cur)

	require.NoError(t, Config{MaxOpenFiles: 64}.CapOpenFiles())
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &after))
	assert.Equal(t, uint64(64), after.Cur)
	assert.Equal(t, before.Max, after.Max, "the hard limit is not changed")
}

// startWorker runs a worker in the background. The returned channel yields the result of Work once it returns.
func startWorker(ctx context.Context, config Config, index int) chan error {
	done := make(chan error, 1)
	go func() { done <- Work(ctx, config, WorkerHostname(index), io.Discard) }()
	return done
}

func TestLaunch_DismissesWorkersOnSuccess(t *testing.T) {
	config := testConfig(t, 2)
	config.DismissTimeout = 10 * time.Second
	config.SettleDelay = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	workers := []chan error{startWorker(ctx, config, 0), startWorker(ctx, config, 1)}
	logs := &lockedBuffer{}

	start := time.Now()
	exitCode, err := Launch(ctx, config, []string{"true"}, localResolver, logs)

	require.NoError(t, err)
	assert.Equal(t, 0, exitCode)
	assert.Contains(t, logs.String(), "all workers have ended")
	// Launch only returns once every worker has ended, and a dismissed worker ends successfully.
	for i, worker := range workers {
		select {
		case err := <-worker:
			assert.NoError(t, err, "worker %d", i)
		default:
			assert.Fail(t, "the launcher returned before the worker ended", "worker %d", i)
		}
	}
	assert.GreaterOrEqual(t, time.Since(start), config.SettleDelay, "the launcher waits before it ends itself")
}

func TestLaunch_LeavesWorkersWhenCommandFails(t *testing.T) {
	config := testConfig(t, 1)
	config.DismissTimeout = 10 * time.Second
	config.SettleDelay = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	worker := startWorker(ctx, config, 0)

	start := time.Now()
	exitCode, err := Launch(ctx, config, []string{"sh", "-c", "exit 5"}, localResolver, io.Discard)

	require.NoError(t, err)
	assert.Equal(t, 5, exitCode)
	assert.Less(t, time.Since(start), 5*time.Second, "a launcher that failed ends at once")
	// The worker is not dismissed: Armada stops the workers of a job that failed.
	select {
	case err := <-worker:
		assert.Fail(t, "the worker ended", "result: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestLaunch_GivesUpOnWorkersThatDoNotTakeTheirDismissal(t *testing.T) {
	config := testConfig(t, 2)
	config.DismissTimeout = 300 * time.Millisecond
	config.SettleDelay = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// The first worker stands by like any other. The second one calls home but then never reads what the
	// launcher sends nor hangs up, as a worker that is stuck would.
	startWorker(ctx, config, 0)
	go func() {
		for ctx.Err() == nil {
			conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(config.Port)))
			if err != nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			defer conn.Close()
			_ = newChannel(conn).send(message{Hostname: WorkerHostname(1)})
			<-ctx.Done()
		}
	}()
	logs := &lockedBuffer{}

	start := time.Now()
	exitCode, err := Launch(ctx, config, []string{"true"}, localResolver, logs)

	require.NoError(t, err)
	assert.Equal(t, 0, exitCode, "the job succeeded even though a worker did not end")
	assert.Less(t, time.Since(start), 5*time.Second, "the launcher does not wait to settle for workers it gave up on")
	assert.Contains(t, logs.String(), "1 workers had not ended")
}
