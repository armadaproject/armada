package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/magefile/mage/mg"
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

// PopulateLookoutTest populates the lookout test
func PopulateLookoutTest() error {
	dockerNet, err := dockerNet()
	if err != nil {
		return err
	}
	if err = dockerRun("ps", "-q", "-f", "name=postgres"); err == nil {

		if err := dockerRun("stop", "postgres"); err != nil {
			return err
		}
		if err := dockerRun("rm", "postgres"); err != nil {
			return err
		}
	}

	err = dockerRun("run", "-d", "--name=postgres", dockerNet, "-p", "5432:5432", "-e", "POSTGRES_PASSWORD=psw", "postgres:14.2")
	if err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	err = goRun("test", "-v", "${PWD}/internal/lookout/db-gen/")
	if err != nil {
		return err
	}

	return nil
}
