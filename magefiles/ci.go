package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

func createQueue() error {
	return runArmadaCtlIgnoreExists("create", "queue", "e2e-test-queue")
}

func createRbacQueue() error {
	return runArmadaCtlIgnoreExists("create", "queue", "rbac-queue", "--owners", "nobody")
}

// createRetryPolicyAndQueue creates the retry policy and the queue that uses
// it. The retry/ testcases submit to this queue. The executor config sets the
// action Delete on the categories that this policy retries. The executor thus
// removes the failed pod, and the retry reuses the name of the pod.
func createRetryPolicyAndQueue() error {
	policyPath, err := writeRetryPolicyFile()
	if err != nil {
		return fmt.Errorf("failed to stage retry policy file: %w", err)
	}
	defer os.Remove(policyPath)

	out, err := runArmadaCtl("create", "retry-policy", "-f", policyPath)
	if err != nil && strings.Contains(out, "already exists") {
		// The policy can remain from an earlier run against a live server.
		// The update makes sure the testcases see the current rules.
		out, err = runArmadaCtl("update", "retry-policy", "-f", policyPath)
	}
	if err != nil {
		fmt.Println(out)
		return err
	}

	if err := runArmadaCtlIgnoreExists("create", "queue", "e2e-retry-queue", "--retry-policies", "e2e-retry-policy"); err != nil {
		return err
	}
	// The scheduler's queue and policy caches poll on queueRefreshPeriod
	// (3s in the local config) and do not observe creations. Wait one
	// period, so the first testcase always finds the policy in the cache.
	time.Sleep(4 * time.Second)
	return nil
}

func writeRetryPolicyFile() (string, error) {
	const policy = `apiVersion: armadaproject.io/v1beta1
kind: RetryPolicy
name: e2e-retry-policy
retryLimit: 2
defaultAction: Fail
rules:
  - action: Retry
    onCategory: "user_error"
`
	f, err := os.CreateTemp("", "retry-policy-*.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.WriteString(policy); err != nil {
		return "", err
	}
	return f.Name(), nil
}

// switchToAuthConfig moves `server`, `scheduler`, and `executor` onto the auth config before the
// rbac suite runs.
func switchToAuthConfig() error {
	envVars := map[string]string{
		"ARMADA_SERVER_CONFIG":               "../server/config-auth.yaml",
		"ARMADA_SCHEDULER_CONFIG":            "../scheduler/config-auth.yaml",
		"ARMADA_EXECUTOR_CONFIG":             "../executor/config-auth.yaml",
		"ARMADA_SERVER_OIDC_PROVIDER_URL":    "http://keycloak:8180/realms/armada",
		"ARMADA_SCHEDULER_OIDC_PROVIDER_URL": "http://keycloak:8180/realms/armada",
		"ARMADA_EXECUTOR_OIDC_PROVIDER_URL":  "http://keycloak:8180/realms/armada",
	}
	for k, v := range envVars {
		os.Setenv(k, v)
	}
	if err := dockerRun("compose", "-f", fullComposeFile, "up", "-d", "--force-recreate", "--wait", "server", "scheduler", "executor"); err != nil {
		return err
	}
	if err := CheckDockerContainerRunning("server", "Armada gRPC server listening on"); err != nil {
		return err
	}
	if err := CheckDockerContainerRunning("scheduler", "Retrieved [1-9]+ executors"); err != nil {
		return err
	}
	if err := CheckDockerContainerRunning("executor", "Reporting current free resource"); err != nil {
		return err
	}
	return nil
}

func runTests(suites []string) error {
	for i, suite := range suites {
		var tests []string
		label := suite
		if info, err := os.Stat(suite); err == nil && !info.IsDir() {
			tests = []string{suite}
			label = strings.TrimSuffix(filepath.Base(suite), filepath.Ext(suite))
		} else {
			tests = []string{fmt.Sprintf("testsuite/testcases/%s/*", suite)}
		}
		timeTaken := time.Now()
		out, err := goOutput("run", "cmd/testsuite/main.go", "test",
			"--tests", strings.Join(tests, ","),
			"--junit", fmt.Sprintf("junit-%s.xml", label),
			"--config", "_local/.armadactl.yaml",
		)
		fmt.Println(out)
		if err != nil {
			return err
		}
		verb := "Time"
		if i > 0 {
			verb = "Additional time"
		}
		fmt.Printf("(Real) %s to run %s tests: %s\n\n", verb, suite, time.Since(timeTaken))
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
	timeTakenTestSuite := time.Now()

	suites := []string{
		"basic", "categorization", "retry",
		"preemption", "reprioritization", "queue",
		"testsuite/testcases/node/node_cancel_by_name_1x5.yaml",
		"testsuite/testcases/node/node_preempt_by_name_1x5.yaml",
	}

	if err := runTests(suites); err != nil {
		return err
	}

	authSuites := []string{
		"rbac",
	}

	if err := switchToAuthConfig(); err != nil {
		return err
	}
	err := runTests(authSuites)
	if err != nil {
		return err
	}

	fmt.Printf("(Real) Total time to run all tests: %s\n\n", time.Since(timeTakenTestSuite))
	return nil
}

// Checks if Armada is ready to accept jobs.
func CheckForArmadaRunning() error {
	// This is a bit of a shonky check, it confirms the scheduler is up and receiving reports from the executor
	//  at which point the system should be ready
	// TODO Make a good check to confirm the system is ready, such as seeing armadactl get executors return a value
	mg.Deps(CheckSchedulerReady)
	mg.Deps(createQueue)
	mg.Deps(createRetryPolicyAndQueue)
	mg.Deps(createRbacQueue)

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
	return runArmadaCtlContext("", args...)
}

// runArmadaCtlContext is runArmadaCtl with an explicit armadactl context override (e.g.
// "rbac-admin"), needed to talk to `server` while it's running the auth config rather than the
// default unauthenticated config every other suite uses -- see switchServiceConfig.
func runArmadaCtlContext(context string, args ...string) (string, error) {
	armadaCtlArgs := []string{
		"--config", "_local/.armadactl.yaml",
	}
	if context != "" {
		armadaCtlArgs = append(armadaCtlArgs, "--context", context)
	}
	armadaCtlArgs = append(armadaCtlArgs, args...)
	outBytes, err := exec.Command(findOrBuildArmadaCtl(), armadaCtlArgs...).CombinedOutput()
	out := string(outBytes)
	return out, err
}

// runArmadaCtlIgnoreExists runs armadactl and accepts an "already exists"
// error. A rerun against a live server thus keeps the resource from the
// earlier run.
func runArmadaCtlIgnoreExists(args ...string) error {
	out, err := runArmadaCtl(args...)
	if err != nil && !strings.Contains(out, "already exists") {
		fmt.Println(out)
		return err
	}

	return nil
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
