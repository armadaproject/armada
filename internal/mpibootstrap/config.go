package mpibootstrap

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// Environment variables set by Armada on the pods of an MPI job.
	WorkersEnvVar        = "ARMADA_MPI_WORKERS"
	SlotsPerWorkerEnvVar = "ARMADA_MPI_SLOTS_PER_WORKER"
	PortEnvVar           = "ARMADA_MPI_PORT"
	DirEnvVar            = "ARMADA_MPI_DIR"
	ReadyTimeoutEnvVar   = "ARMADA_MPI_READY_TIMEOUT"
	MaxOpenFilesEnvVar   = "ARMADA_MPI_MAX_OPEN_FILES"
	// GangServiceNameEnvVar is set by the executor on all pods of a gang with a gang service.
	GangServiceNameEnvVar = "ARMADA_GANG_SERVICE_NAME"

	// LauncherHostname is the hostname of the launcher pod, and WorkerHostnamePrefix that of the worker pods.
	LauncherHostname     = "launcher"
	WorkerHostnamePrefix = "worker-"

	DefaultPort         = 9477
	DefaultDir          = "/opt/armada-mpi"
	DefaultReadyTimeout = 10 * time.Minute
	// DefaultMaxOpenFiles is the default of Config.MaxOpenFiles.
	DefaultMaxOpenFiles = 65536

	// BinaryName and AgentBinaryName are the names under which the armada-mpi program is installed in DefaultDir.
	// MPI launchers that take the path of a remote shell, but no arguments for it, are given the latter.
	BinaryName      = "armada-mpi"
	AgentBinaryName = "armada-mpi-agent"

	agentSocketName = "agent.sock"
	// OpenMpiHostfileName and HydraHostfileName are the hostfiles Launch writes into the directory of Config.
	OpenMpiHostfileName = "hostfile"
	HydraHostfileName   = "hostfile.hydra"
)

// Config describes an MPI job to the launcher and the workers.
type Config struct {
	// Workers is the number of worker pods.
	Workers int
	// SlotsPerWorker is the number of MPI processes each worker may run.
	SlotsPerWorker int
	// Port is the port of the launcher that the workers call home to.
	Port int
	// Dir is a directory private to the container. It holds the hostfiles and the socket of the agent.
	Dir string
	// ServiceName is the name of the gang service under which the pods of the job are published.
	// If empty, the pods are reached by their bare hostnames.
	ServiceName string
	// ReadyTimeout is how long the launcher waits for all workers to call home before giving up.
	ReadyTimeout time.Duration
	// MaxOpenFiles caps the soft limit on open files of the MPI processes. Zero leaves the limit alone.
	// Container runtimes may set that limit to a billion or more, and some MPI versions, notably Open MPI up to
	// version 4, walk through every possible file descriptor each time they start a process, which then takes
	// minutes. Processes that need more may still raise their limit up to the hard limit, which is not changed.
	MaxOpenFiles int

	// launcherHost overrides the address at which the workers reach the launcher. It is only set in tests.
	launcherHost string
}

// ConfigFromEnv reads the Config from the environment Armada sets on the pods of an MPI job.
func ConfigFromEnv() (Config, error) {
	config := Config{
		SlotsPerWorker: 1,
		Port:           DefaultPort,
		Dir:            DefaultDir,
		ServiceName:    os.Getenv(GangServiceNameEnvVar),
		ReadyTimeout:   DefaultReadyTimeout,
		MaxOpenFiles:   DefaultMaxOpenFiles,
	}
	var err error
	if config.Workers, err = intFromEnv(WorkersEnvVar, 0); err != nil {
		return config, err
	}
	if config.SlotsPerWorker, err = intFromEnv(SlotsPerWorkerEnvVar, config.SlotsPerWorker); err != nil {
		return config, err
	}
	if config.Port, err = intFromEnv(PortEnvVar, config.Port); err != nil {
		return config, err
	}
	if config.MaxOpenFiles, err = intFromEnv(MaxOpenFilesEnvVar, config.MaxOpenFiles); err != nil {
		return config, err
	}
	if dir := os.Getenv(DirEnvVar); dir != "" {
		config.Dir = dir
	}
	if timeout := os.Getenv(ReadyTimeoutEnvVar); timeout != "" {
		if config.ReadyTimeout, err = time.ParseDuration(timeout); err != nil {
			return config, fmt.Errorf("invalid %s: %w", ReadyTimeoutEnvVar, err)
		}
	}
	return config, nil
}

func intFromEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid %s %q: must be a non-negative integer", name, value)
	}
	return parsed, nil
}

// WorkerHostname returns the hostname of the worker with the given index.
func WorkerHostname(index int) string {
	return WorkerHostnamePrefix + strconv.Itoa(index)
}

// address returns the name under which the pod with the given hostname is reached by the other pods of the job.
func (c Config) address(hostname string) string {
	if c.ServiceName == "" {
		return hostname
	}
	return hostname + "." + c.ServiceName
}

func (c Config) agentSocket() string {
	return filepath.Join(c.Dir, agentSocketName)
}

// shortHostname strips any domain from a host name: MPI launchers may refer to a worker by its address.
func shortHostname(host string) string {
	short, _, _ := strings.Cut(host, ".")
	return short
}

// writeHostfiles writes the list of workers in the formats of Open MPI and of the Hydra process manager, which is
// used by MPICH and Intel MPI. The launcher is not listed: it only runs mpirun.
func (c Config) writeHostfiles() error {
	var openMpi, hydra strings.Builder
	for i := 0; i < c.Workers; i++ {
		address := c.address(WorkerHostname(i))
		fmt.Fprintf(&openMpi, "%s slots=%d\n", address, c.SlotsPerWorker)
		fmt.Fprintf(&hydra, "%s:%d\n", address, c.SlotsPerWorker)
	}
	if err := os.WriteFile(filepath.Join(c.Dir, OpenMpiHostfileName), []byte(openMpi.String()), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(c.Dir, HydraHostfileName), []byte(hydra.String()), 0o644)
}
