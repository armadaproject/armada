package main

import (
	"fmt"
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

	start := time.Now()
	err := dockerComposeRun("up", "-d", "redis", "postgres", "pulsar")
	if err != nil {
		return err
	}
	delta := time.Since(start)
	fmt.Printf("Redis, Pulsar and Postgres took %s\n", delta)

	mg.Deps(CheckForPulsarRunning)

	start = time.Now()
	// By starting the executor first,
	// we can ensure that the server will be able to register the executor cluster
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
	delta = time.Since(start)
	fmt.Printf("components + queue creation took %s\n", delta)

	return nil
}

// Build images, spin up a test environment, and run the integration tests against it.
func ciRunTests() error {
	mg.Deps(CheckForArmadaRunning)

	out, err := goOutput("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	fmt.Println(out)
	if err != nil {
		return err
	}
	return nil
}

func CheckForArmadaRunning() error {
	timeout := time.After(1 * time.Minute)
	tick := time.Tick(1 * time.Second)
	seconds := 0
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for Armada to start")
		case <-tick:
			outbytes, _ := exec.Command(goBinary(), "run", "cmd/armadactl/main.go", "submit", "./developer/config/job.yaml").CombinedOutput()
			out := string(outbytes)
			if !strings.Contains(out, "no executor clusters available") && !strings.Contains(out, "connection refused") {
				// Sleep for 1 second to allow Armada to fully start
				time.Sleep(1 * time.Second)
				fmt.Printf("\nArmada took %d seconds to start!\n\n", seconds)
				return nil
			}
			seconds++
		}
	}
}
