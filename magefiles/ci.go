package main

import (
	"os"
	"time"

	"github.com/avast/retry-go"
	"github.com/magefile/mage/sh"
)

// Build images, spin up a test environment, and run the integration tests against it.
func ciRunTests() error {
	if err := os.MkdirAll(".kube", os.ModeDir|0o755); err != nil {
		return err
	}
	err := dockerComposeRun("up", "-d", "redis", "postgres", "pulsar", "stan")
	if err != nil {
		return err
	}

	// TODO: Necessary to avoid connection error on Armada server startup.
	time.Sleep(10 * time.Second)
	err = dockerComposeRun("up", "-d", "server", "executor")
	if err != nil {
		return err
	}

	retry.Do(
		func() error {

			sh.Run("docker", "ps")

			err = goRun("run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue")
			if err != nil {
				return err
			}
			return nil
		},
		retry.Delay(time.Second),
	)

	err = goRun("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	if err != nil {
		return err
	}
	return nil
}
