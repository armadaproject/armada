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

func createQueue() error {
	out, err := runArmadaCtl("create", "queue", "e2e-test-queue")
	// check if err text contains "already exists" and ignore if it does
	if err != nil && !strings.Contains(out, "already exists") {
		fmt.Println(out)
		return err
	}

	return nil
}

// Build images, spin up a test environment, and run the integration tests against it.
func TestSuite() error {
	mg.Deps(CheckForArmadaRunning)

	// Only set these if they have not already been set
	if os.Getenv("ARMADA_EXECUTOR_INGRESS_URL") == "" {
		os.Setenv("ARMADA_EXECUTOR_INGRESS_URL", "http://localhost")
	}
	if os.Getenv("ARMADA_EXECUTOR_INGRESS_PORT") == "" {
		os.Setenv("ARMADA_EXECUTOR_INGRESS_PORT", "5001")
	}

	timeTaken := time.Now()
	out, err2 := goOutput("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
		"--config", "e2e/config/armadactl_config.yaml",
	)
	if err2 != nil {
		fmt.Println(out)
		return err2
	}
	fmt.Printf("(Real) Time to run tests: %s\n\n", time.Since(timeTaken))

	fmt.Println(out)
	return nil
}

// Checks if Armada is ready to accept jobs.
func CheckForArmadaRunning() error {
	// This is a bit of a shonky check, it confirms the scheduler is up and receiving reports from the executor
	//  at which point the system should be ready
	// TODO Make a good check to confirm the system is ready, such as seeing armadactl get executors return a value
	mg.Deps(CheckSchedulerReady)
	mg.Deps(createQueue)

	// Set high to take compile time into account
	timeout := time.After(2 * time.Minute)
	tick := time.Tick(1 * time.Second)
	seconds := 0
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for Armada to start")
		case <-tick:
			out, _ := runArmadaCtl("submit", "./developer/config/job.yaml")
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

func runArmadaCtl(args ...string) (string, error) {
	armadaCtlArgs := []string{
		"--config", "e2e/config/armadactl_config.yaml",
	}
	armadaCtlArgs = append(armadaCtlArgs, args...)
	outBytes, err := exec.Command(findOrBuildArmadaCtl(), armadaCtlArgs...).CombinedOutput()
	out := string(outBytes)
	return out, err
}

// Builds armadactl binary using goreleaser and returns the path.
func buildArmadactl() (string, error) {
	err := goreleaserRun("build", "--id", "armadactl", "--single-target", "--snapshot", "--clean")
	if err != nil {
		return "", err
	}

	output, err := sh.Output("sh", "-c", "find dist -name armadactl -type f -print -quit")
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(output), nil
}

// Finds armadactl to submit with, building from source if necessary.
func findOrBuildArmadaCtl() string {
	// Check dist/ for already-built binary
	if output, err := sh.Output("sh", "-c", "find dist -name armadactl -type f -print -quit 2>/dev/null"); err == nil {
		if path := strings.TrimSpace(output); path != "" {
			return path
		}
	}

	// Check local directory
	if _, err := os.Stat("./armadactl"); err == nil {
		return "./armadactl"
	}

	// Check PATH
	if path, err := exec.LookPath("armadactl"); err == nil {
		return path
	}

	// Build from source
	path, err := buildArmadactl()
	if err != nil {
		return ""
	}
	return path
}
