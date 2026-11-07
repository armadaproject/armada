package main

import (
	"bytes"
	"debug/buildinfo"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var Gotestsum string

var LocalBin = filepath.Join(os.Getenv("PWD"), "/bin")

var (
	redisImage    = "redis:6.2.6"
	postgresImage = "postgres:14.2"
)

// testPackageParallelism caps how many packages go test runs at once. Many
// tests spin up a fresh Postgres database and apply the full migration chain,
// so running every package in parallel overwhelms the shared test postgres and
// causes context deadline exceeded flakes. 1 runs the whole suite serially,
// which is slow but reliable even on loaded dev machines; raise it on a lighter
// machine to trade some reliability for speed. See Tests.
const testPackageParallelism = "1"

func makeLocalBin() error {
	if _, err := os.Stat(LocalBin); os.IsNotExist(err) {
		err = os.MkdirAll(LocalBin, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

const (
	gotestsumModule  = "gotest.tools/gotestsum"
	gotestsumVersion = "v1.13.0"
)

// gotestsum ensures that tests use the pinned gotestsum binary.
func gotestsum() error {
	binary := filepath.Join(LocalBin, "gotestsum")
	if err := ensureGotestsum(binary, isPinnedGotestsum, installGotestsum); err != nil {
		return err
	}
	Gotestsum = binary
	return nil
}

func installGotestsum(binDir string) error {
	fmt.Println("Installing", gotestsumModule+"@"+gotestsumVersion)
	cmd := exec.Command("go", "install", gotestsumModule+"@"+gotestsumVersion)
	cmd.Env = append(os.Environ(), "GOBIN="+binDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// isPinnedGotestsum reads metadata without executing an unverified local tool.
// Read the file first so filesystem failures remain errors even when the build
// info reader would report them as an unrecognized executable format.
func isPinnedGotestsum(binary string) (bool, error) {
	info, err := os.Stat(binary)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("%s is not a regular file", binary)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		return false, err
	}
	metadata, err := buildinfo.Read(bytes.NewReader(data))
	if err != nil {
		// Non-Go and damaged binaries are cache misses.
		return false, nil
	}
	return info.Mode()&0o111 != 0 && isPinnedGotestsumBuild(metadata), nil
}

func isPinnedGotestsumBuild(metadata *debug.BuildInfo) bool {
	return metadata.Path == gotestsumModule && metadata.Main.Path == gotestsumModule &&
		metadata.Main.Version == gotestsumVersion && metadata.Main.Replace == nil
}

// ensureGotestsum stages and verifies updates before replacing the managed tool.
// The callbacks keep bootstrap regressions independent of the network and Mage's
// dependency memoization; production uses the same path with the real installer.
func ensureGotestsum(binary string, matches func(string) (bool, error), install func(string) error) (err error) {
	matching, err := matches(binary)
	if err != nil {
		return fmt.Errorf("checking gotestsum: %w", err)
	}
	if matching {
		return nil
	}
	binDir := filepath.Dir(binary)
	if err := os.MkdirAll(binDir, os.ModePerm); err != nil {
		return fmt.Errorf("creating gotestsum directory: %w", err)
	}
	staging, err := os.MkdirTemp(binDir, ".gotestsum-*")
	if err != nil {
		return fmt.Errorf("staging gotestsum: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(staging); cleanupErr != nil && err == nil {
			err = fmt.Errorf("cleaning up gotestsum installation: %w", cleanupErr)
		}
	}()

	if err := install(staging); err != nil {
		return fmt.Errorf("installing gotestsum: %w", err)
	}
	installed := filepath.Join(staging, "gotestsum")
	matching, err = matches(installed)
	if err != nil {
		return fmt.Errorf("verifying installed gotestsum: %w", err)
	}
	if !matching {
		return fmt.Errorf("installed gotestsum is not %s@%s", gotestsumModule, gotestsumVersion)
	}
	if err := os.Rename(installed, binary); err != nil {
		return fmt.Errorf("replacing gotestsum: %w", err)
	}
	return nil
}

// Tests is a mage target that runs the tests and generates coverage reports.
func Tests() error {
	mg.Deps(gotestsum)
	var err error

	docker_Net, err := dockerNet()
	if err != nil {
		return err
	}

	redisArgs := []string{"run", "-d", "--name=redis", "-p=6379:6379"}
	if len(docker_Net) > 0 {
		redisArgs = append(redisArgs, docker_Net)
	}
	redisArgs = append(redisArgs, redisImage)
	err = dockerRun(redisArgs...)
	if err != nil {
		return err
	}

	postgresArgs := []string{"run", "-d", "--name=postgres", "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=psw"}
	if len(docker_Net) > 0 {
		postgresArgs = append(postgresArgs, docker_Net)
	}
	postgresArgs = append(postgresArgs, postgresImage)
	err = dockerRun(postgresArgs...)
	if err != nil {
		return err
	}

	defer func() {
		if err := dockerRun("rm", "-f", "redis", "postgres"); err != nil {
			fmt.Println(err)
		}
	}()

	err = sh.Run("sleep", "3")
	if err != nil {
		return err
	}
	packages, err := sh.Output("go", "list", "./internal/...")
	if err != nil {
		return err
	}

	cmd := []string{
		"--format", "short-verbose",
		"--junitfile", "test-reports/unit-tests.xml",
		"--jsonfile", "test-reports/unit-tests.json",
		"--no-color=false",
		// Cap concurrent package execution so the many Postgres-backed tests
		// (each spins up a fresh database and runs the full migration chain)
		// don't overwhelm the shared test postgres container and blow their
		// context timeouts.
		"--", "-p", testPackageParallelism, "-coverprofile=test-reports/coverage.out",
		"-covermode=atomic", "./cmd/...",
		"./pkg/...",
	}
	cmd = append(cmd, strings.Fields(packages)...)

	testCmd := exec.Command(Gotestsum, cmd...)

	// If -verbose was set, we let os.Stdout handles the output.
	// Otherwise, we need to capture the tests output and print it in the case of failures.
	var buffer bytes.Buffer
	if os.Getenv("MAGEFILE_VERBOSE") == "1" {
		testCmd.Stdout = os.Stdout
	} else {
		testCmd.Stdout = &buffer
	}

	if err := testCmd.Run(); err != nil {
		if os.Getenv("MAGEFILE_VERBOSE") == "0" {
			fmt.Println(buffer.String())
		}
		return err
	}

	return err
}

func runTest(name, outputFileName string) error {
	cmd := exec.Command(Gotestsum, "--", "-v", name, "-count=1", "-p", testPackageParallelism)
	file, err := os.Create(filepath.Join("test_reports", outputFileName))
	if err != nil {
		return err
	}
	defer file.Close()
	cmd.Stdout = io.MultiWriter(os.Stdout, file)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Teste2eAirflow runs e2e tests for airflow
func Teste2eAirflow() error {
	mg.Deps(AirflowOperator)
	// Waits for Armada to be ready and creates the e2e-test-queue the tests submit to.
	mg.Deps(CheckForArmadaRunning)

	err := dockerRun("run", "-v", "${PWD}/e2e:/e2e", "-v", "${PWD}/third_party/airflow:/code",
		"--workdir", "/code", "-e", "ARMADA_SERVER=server", "-e", "ARMADA_PORT=50051", "--entrypoint",
		"python3", "--network=kind", "armada-airflow-operator-builder:latest",
		"-m", "pytest", "-v", "-s", "/code/test/integration/test_airflow_operator_logic.py")
	if err != nil {
		return err
	}

	return nil
}

// Teste2epython runs e2e tests for python client
func Teste2epython() error {
	mg.Deps(BuildPython)
	mg.Deps(CheckForArmadaRunning)
	args := []string{
		"run",
		"-v", "${PWD}/client/python:/code",
		"--workdir", "/code",
		"-e", "ARMADA_SERVER=server",
		"-e", "ARMADA_PORT=50051",
		"--entrypoint", "python3",
		"--network", "kind",
		"armada-python-client-builder:latest",
		"-m", "pytest",
		"-v", "-s",
		"/code/tests/integration/test_no_auth.py",
	}

	return dockerRun(args...)
}

// TestsNoSetup runs the tests without setup
func TestsNoSetup() error {
	mg.Deps(gotestsum)

	if err := runTest("./internal...", "internal.txt"); err != nil {
		return err
	}
	if err := runTest("./pkg...", "pkg.txt"); err != nil {
		return err
	}
	if err := runTest("./cmd...", "cmd.txt"); err != nil {
		return err
	}

	return nil
}
