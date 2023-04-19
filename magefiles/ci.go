package main

import (
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
)

func ciSetup() error {
	if err := os.MkdirAll(".kube", os.ModeDir|0o755); err != nil {
		return err
	}
	err := dockerComposeRun("up", "-d", "redis", "postgres", "pulsar")
	if err != nil {
		return err
	}

	mg.Deps(CheckForPulsarRunning)

	// By starting the executor first,
	// we can ensure that the server will be able to connect to it
	// on its first attempt.
	err = dockerComposeRun("up", "-d", "executor")
	if err != nil {
		return err
	}
	err = dockerComposeRun("up", "-d", "server")
	if err != nil {
		return err
	}

	err = goRun("run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue")
	if err != nil {
		return err
	}

	return nil
}

// Build images, spin up a test environment, and run the integration tests against it.
func ciRunTests() error {

	mg.Deps(checkforArmadaRunning)

	err := goRun("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	if err != nil {
		return err
	}
	return nil
}

// Use this command to test when the server is ready
func checkforArmadaRunning() error {
	// switch to using os exec to hide stdout
	outbytes, _ := exec.Command(goBinary(), "run", "cmd/armadactl/main.go", "submit", "./testsuite/testcases/basic/failure_1x1.yaml").Output()
	out := string(outbytes)

	// wait until connection refused does not appear in out
	for strings.Contains(out, "connection refused") {
		time.Sleep(5 * time.Second)
		outbytes, _ = exec.Command(goBinary(), "run", "cmd/armadactl/main.go", "submit", "./testsuite/testcases/basic/failure_1x1.yaml").Output()
		out = string(outbytes)
	}

	return nil
}
