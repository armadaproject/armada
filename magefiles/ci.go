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

const e2eTestQueue = `apiVersion: armadaproject.io/v1beta1
kind: Queue
name: e2e-test-queue
priorityFactor: 1.0
`

const rbacFixturePlainQueue = `apiVersion: armadaproject.io/v1beta1
kind: Queue
name: rbac-fixture-plain
priorityFactor: 1.0
`

// rbacFixtureRestrictedQueue grants the Keycloak "users" group submit-only, so armada-user's
// negative tests (cancel/reprioritize/watch without permission) can submit a job and prove those
// specific actions -- not queue ownership -- are what's denied.
const rbacFixtureRestrictedQueue = `apiVersion: armadaproject.io/v1beta1
kind: Queue
name: rbac-fixture-restricted
priorityFactor: 1.0
permissions:
  - subjects:
      - name: users
        kind: Group
    verbs:
      - submit
`

func createQueue() error {
	return createQueuesFromStrings("", e2eTestQueue)
}

// createRbacFixtureQueues bootstraps the queues the rbac test suite's "without permission" cases
// target, using the rbac-admin context (server-auth, on the containerized full.yaml stack) since
// armada-user itself lacks create_queue.
func createRbacFixtureQueues() error {
	return createQueuesFromStrings("rbac-admin", rbacFixturePlainQueue, rbacFixtureRestrictedQueue)
}

// createQueuesFromStrings runs armadactl to create each given queue fixture against the given
// armadactl context, tolerating "already exists" so repeated CI runs against a stack that wasn't
// torn down don't fail.
func createQueuesFromStrings(context string, queues ...string) error {
	for _, queue := range queues {
		queuePath, err := writeTempFile("queue-*.yaml", queue)
		if err != nil {
			return fmt.Errorf("failed to stage queue file: %w", err)
		}
		defer os.Remove(queuePath)

		if err := createResource(context, "-f", queuePath); err != nil {
			return err
		}
	}
	return nil
}

// createRetryPolicyAndQueue smoke-tests retry-policy creation and queue
// attachment against a live server: it creates a policy and a queue bound to
// it. Driving a retry end to end additionally needs the executor to delete the
// failed pod so the retry can reuse its name, so no testcase submits to this
// queue yet.
func createRetryPolicyAndQueue() error {
	policyPath, err := writeRetryPolicyFile()
	if err != nil {
		return fmt.Errorf("failed to stage retry policy file: %w", err)
	}
	defer os.Remove(policyPath)

	if err := createResource("", "retry-policy", "-f", policyPath); err != nil {
		return err
	}
	return createResource("", "queue", "e2e-retry-queue", "--retry-policies", "e2e-retry-policy")
}

// createResource runs `armadactl create <args...>` against the given armadactl context,
// tolerating "already exists" so repeated CI runs against a stack that wasn't torn down don't
// fail.
func createResource(context string, args ...string) error {
	out, err := runArmadaCtlContext(context, append([]string{"create"}, args...)...)
	if err != nil && !strings.Contains(out, "already exists") {
		fmt.Println(out)
		return err
	}
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
	return writeTempFile("retry-policy-*.yaml", policy)
}

func writeTempFile(pattern string, contents string) (string, error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.WriteString(contents); err != nil {
		return "", err
	}
	return f.Name(), nil
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
		"basic", "categorization", "preemption", "reprioritization", "queue", "rbac",
		"testsuite/testcases/node/node_cancel_by_name_1x5.yaml",
		"testsuite/testcases/node/node_preempt_by_name_1x5.yaml",
	}

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
	mg.Deps(createRbacFixtureQueues)

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
// "rbac-admin"), needed to bootstrap the rbac suite's fixture queues against server-auth rather
// than the default unauthenticated "server" connection every other suite uses.
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
