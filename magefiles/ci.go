package main

import (
	"os"
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

	err = dockerComposeRun("up", "-d", "server", "executor")
	if err != nil {
		return err
	}

	err = goRun("run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue")
	if err != nil {
		return err
	}

	time.Sleep(15 * time.Second)

	return nil
}

// Build images, spin up a test environment, and run the integration tests against it.
func ciRunTests() error {
	err := goRun("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	if err != nil {
		return err
	}
	return nil
}
