package main

import (
	"os"
	"time"

	"github.com/avast/retry-go"
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

	// Retry on failure. Since it may take time for components to become ready.
	retry.Do(
		func() error {
			if err := dockerComposeRun("up", "-d", "server", "executor"); err != nil {
				return err
			}
			time.Sleep(time.Second)
			if err := goRun("run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue"); err != nil {
				return err
			}
			return nil
		},
		retry.Delay(time.Second),
		retry.Attempts(20),
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
