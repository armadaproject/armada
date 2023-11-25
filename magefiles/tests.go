package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

var Gotestsum string

var LocalBin = filepath.Join(os.Getenv("PWD"), "/bin")

func makeLocalBin() error {
	if _, err := os.Stat(LocalBin); os.IsNotExist(err) {
		err = os.MkdirAll(LocalBin, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

// Gotestsum downloads gotestsum locally if necessary
func gotestsum() error {
	mg.Deps(makeLocalBin)
	Gotestsum = filepath.Join(LocalBin, "/gotestsum")

	if _, err := os.Stat(Gotestsum); os.IsNotExist(err) {
		fmt.Println(Gotestsum)
		cmd := exec.Command("go", "install", "gotest.tools/gotestsum@v1.8.2")
		cmd.Env = append(os.Environ(), "GOBIN="+LocalBin)
		return cmd.Run()

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

	err = dockerRun("run", "-d", "--name=redis", docker_Net, "-p=6379:6379", "redis:6.2.6")
	if err != nil {
		return err
	}

	err = dockerRun("run", "-d", "--name=postgres", docker_Net, "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=psw", "postgres:14.2")
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

	internalPackages := filterPackages(strings.Fields(packages), "jobservice/repository")

	cmd := []string{
		"--format", "short-verbose",
		"--junitfile", "test-reports/unit-tests.xml",
		"--jsonfile", "test-reports/unit-tests.json",
		"--no-color=false",
		"--", "-coverprofile=test-reports/coverage.out",
		"-covermode=atomic", "./cmd/...",
		"./pkg/...",
	}
	cmd = append(cmd, internalPackages...)

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

func filterPackages(packages []string, filter string) []string {
	var filtered []string
	for _, pkg := range packages {
		if !strings.Contains(pkg, filter) {
			filtered = append(filtered, pkg)
		}
	}
	return filtered
}

func runTest(name, outputFileName string) error {
	cmd := exec.Command(Gotestsum, "--", "-v", name, "-count=1")
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
	if err := BuildDockers("jobservice"); err != nil {
		return err
	}

	cmd, err := go_CMD()
	if err != nil {
		return err
	}
	cmd = append(cmd, "go", "run", "cmd/armadactl/main.go", "create", "queue", "queue-a")
	if err := dockerRun(cmd...); err != nil {
		fmt.Println(err)
	}

	if err := dockerRun("rm", "-f", "jobservice"); err != nil {
		fmt.Println(err)
	}

	err = dockerRun("run", "-d", "--name", "jobservice", "--network=kind",
		"--mount", "type=bind,src=${PWD}/e2e,dst=/e2e", "gresearch/armada-jobservice", "run", "--config",
		"/e2e/setup/jobservice.yaml")
	if err != nil {
		return err
	}

	err = dockerRun("run", "-v", "${PWD}/e2e:/e2e", "-v", "${PWD}/third_party/airflow:/code",
		"--workdir", "/code", "-e", "ARMADA_SERVER=server", "-e", "ARMADA_PORT=50051", "-e", "JOB_SERVICE_HOST=jobservice",
		"-e", "JOB_SERVICE_PORT=60003", "--entrypoint", "python3", "--network=kind", "armada-airflow-operator-builder:latest",
		"-m", "pytest", "-v", "-s", "/code/tests/integration/test_airflow_operator_logic.py")
	if err != nil {
		return err
	}

	err = dockerRun("rm", "-f", "jobservice")
	if err != nil {
		return err
	}
	return nil
}

// Teste2epython runs e2e tests for python client
func Teste2epython() error {
	mg.Deps(BuildPython)
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
