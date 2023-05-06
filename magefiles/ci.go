package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Build images, spin up a test environment, and run the integration tests against it.
func TestSuite() error {
	timeTaken := time.Now()

	outbytes, err := exec.Command(armadaCtl(), "create", "queue", "e2e-test-queue").CombinedOutput()
	out := string(outbytes)
	// check if err text contains "already exists" and ignore if it does
	if err != nil && !strings.Contains(out, "already exists") {
		fmt.Println(out)
		return err
	}

	fmt.Printf("Time to create queue: %s\n\n", time.Since(timeTaken))

	mg.Deps(CheckForArmadaRunning)

	timeTaken = time.Now()
	out, err2 := goOutput("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	if err2 != nil {
		return err2
	}
	fmt.Printf("(Real) Time to run tests: %s\n\n", time.Since(timeTaken))

	fmt.Println(out)
	return nil
}

// NOTE: This command assumes that the queue "e2e-test-queue" already exists
func CheckForArmadaRunning() error {
	timeout := time.After(1 * time.Minute)
	tick := time.Tick(1 * time.Second)
	seconds := 0
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for Armada to start")
		case <-tick:
			outbytes, _ := exec.Command(armadaCtl(), "submit", "./developer/config/job.yaml").CombinedOutput()
			out := string(outbytes)
			if strings.Contains(out, "Submitted job with id") {
				// Sleep for 1 second to allow Armada to fully start
				time.Sleep(1 * time.Second)
				fmt.Printf("\nArmada took %d seconds to start!\n\n", seconds)
				return nil
			}
			seconds++
		}
	}
}

func armadaCtl() string {
	if _, err := os.Stat("./armadactl"); os.IsNotExist(err) {
		err = sh.Run("sh", "./docs/local/armadactl.sh")
		if err != nil {
			return ""
		}
	}
	return "./armadactl"
}
