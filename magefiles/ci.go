package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
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

	// Basic and categorization tests — all run in parallel, no ordering constraints.
	out, err := goOutput("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*,testsuite/testcases/categorization/*",
		"--junit", "junit.xml",
		"--config", "_local/.armadactl.yaml",
	)
	fmt.Println(out)
	if err != nil {
		return err
	}

	// Node preempt must run before node cancel: CancelOnNode kills all jobs on the node,
	// which would cause the preempt test to receive a Cancelled event instead of Preempted.
	out, err = goOutput("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/node/node_preempt_by_tag_1x1.yaml",
		"--junit", "junit-node-preempt.xml",
		"--config", "_local/.armadactl.yaml",
	)
	fmt.Println(out)
	if err != nil {
		return err
	}

	out, err = goOutput("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/node/node_cancel_by_name_1x1.yaml",
		"--junit", "junit-node-cancel.xml",
		"--config", "_local/.armadactl.yaml",
	)
	fmt.Println(out)
	if err != nil {
		return err
	}

	fmt.Printf("(Real) Time to run tests: %s\n\n", time.Since(timeTaken))
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
			out, _ := runArmadaCtl("submit", "./_local/readiness-job.yaml")
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

// CheckSchedulerReady waits until the scheduler reports at least one registered executor.
func CheckSchedulerReady() error {
	return CheckDockerContainerRunning("scheduler", "Retrieved [1-9]+ executors")
}

// CheckDockerContainerRunning repeatedly checks a container's logs until expectedLogRegex matches.
func CheckDockerContainerRunning(containerName string, expectedLogRegex string) error {
	timeout := time.After(1 * time.Minute)
	tick := time.Tick(1 * time.Second)
	seconds := 0

	logMatchRegex, err := regexp.Compile(expectedLogRegex)
	if err != nil {
		return fmt.Errorf("invalid log regex %s - %s", expectedLogRegex, err)
	}

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for %s to start", containerName)
		case <-tick:
			out, err := dockerOutput("compose", "-f", "_local/compose/full.yaml", "logs", containerName)
			if err != nil {
				return err
			}
			if len(logMatchRegex.FindStringSubmatch(out)) > 0 {
				if seconds < 1 {
					fmt.Printf("\n%s had already started!\n\n", containerName)
					return nil
				}
				fmt.Printf("\n%s took %d seconds to start!\n\n", containerName, seconds)
				return nil
			}
			seconds++
		}
	}
}

func runArmadaCtl(args ...string) (string, error) {
	armadaCtlArgs := []string{
		"--config", "_local/.armadactl.yaml",
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
