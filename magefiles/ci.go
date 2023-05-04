package main

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
)

// Build images, spin up a test environment, and run the integration tests against it.
func TestSuite() error {
	mg.Deps(CheckForArmadaRunning)

	err := goRun("run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue")
	if err != nil {
		return err
	}

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
	timeout := time.After(2 * time.Minute)
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
