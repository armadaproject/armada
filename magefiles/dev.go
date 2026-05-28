package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Dev namespace: unified entry point for Armada local development & e2e.
//
// Both `dev:up` (humans) and `dev:e2e` (CI) share the same execution model:
// dependencies in containers (via _local/compose/stack.yaml), Armada components
// as host processes via goreman with the per-component configs in _local/.
type Dev mg.Namespace

const (
	goremanPackage   = "github.com/mattn/goreman@v0.3.15"
	stackComposeFile = "_local/compose/stack.yaml"
	initScript       = "_local/scripts/init.sh"
	defaultProcfile  = "_local/procfiles/no-auth.Procfile"
	readinessJob     = "testsuite/testcases/basic/submit_1x1.yaml"
	armadactlConfig  = "_local/.armadactl.yaml"
	queueName        = "e2e-test-queue"
)

// Up brings up dependencies and runs Armada components via goreman with the chosen procfile.
// Profile: no-auth (default), auth, or fake-executor.
//
// Examples:
//
//	mage dev:up                # no-auth
//	mage dev:up auth           # OIDC
//	mage dev:up fake-executor  # no Kubernetes needed
func (Dev) Up(profile string) error {
	if profile == "" {
		profile = "no-auth"
	}
	procfile := "_local/procfiles/" + profile + ".Procfile"
	if _, err := os.Stat(procfile); err != nil {
		return fmt.Errorf("unknown profile %q: %s not found", profile, procfile)
	}

	mg.Deps(installGoreman)
	if err := devDepsUp(); err != nil {
		return err
	}
	if err := sh.RunV(initScript); err != nil {
		return err
	}
	return sh.RunV(goremanBin(), "-f", procfile, "start")
}

// Deps brings up only the docker-compose dependencies (redis, postgres, pulsar).
// Useful when running goreman manually in another terminal.
func (Dev) Deps() error {
	return devDepsUp()
}

// Down stops docker-compose dependencies. If a `dev:up` goreman is running, Ctrl+C it first.
func (Dev) Down() error {
	return sh.RunV("docker", "compose", "-f", stackComposeFile, "down")
}

// Testsuite runs the integration test suite end-to-end: Kind cluster + deps + Armada via
// goreman (background) + testsuite, then tears everything down. What CI calls.
func (Dev) Testsuite() error {
	mg.Deps(installGoreman)
	mg.Deps(Kind)
	if err := devDepsUp(); err != nil {
		return err
	}
	if err := sh.RunV(initScript); err != nil {
		return err
	}

	goreman := exec.Command(goremanBin(), "-f", defaultProcfile, "start")
	goreman.Stdout = os.Stdout
	goreman.Stderr = os.Stderr
	if err := goreman.Start(); err != nil {
		return fmt.Errorf("start goreman: %w", err)
	}
	defer func() {
		if goreman.Process != nil {
			_ = goreman.Process.Kill()
		}
	}()

	if err := waitForArmadaReady(2 * time.Minute); err != nil {
		return err
	}
	return runTestsuite()
}

func devDepsUp() error {
	return sh.RunV("docker", "compose", "-f", stackComposeFile, "up", "-d", "--wait")
}

func goremanBin() string {
	return filepath.Join(LocalBin, "goreman")
}

func installGoreman() error {
	mg.Deps(makeLocalBin)
	if _, err := os.Stat(goremanBin()); err == nil {
		return nil
	}
	fmt.Println("Installing", goremanPackage, "to", goremanBin())
	cmd := exec.Command("go", "install", goremanPackage)
	cmd.Env = append(os.Environ(), "GOBIN="+LocalBin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// waitForArmadaReady polls armadactl submit until Armada accepts a job, which proves
// server is up, scheduler has registered an executor, and the queue is usable.
func waitForArmadaReady(timeout time.Duration) error {
	armadactl := findOrBuildArmadaCtl()
	if armadactl == "" {
		return fmt.Errorf("no armadactl binary available")
	}

	// Ensure queue exists (idempotent).
	createOut, _ := exec.Command(armadactl, "--config", armadactlConfig, "create", "queue", queueName).CombinedOutput()
	if s := string(createOut); s != "" && !strings.Contains(s, "already exists") && !strings.Contains(s, "Successfully") {
		fmt.Println(s)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, _ := exec.Command(armadactl, "--config", armadactlConfig, "submit", readinessJob).CombinedOutput()
		if strings.Contains(string(out), "Submitted job with id") {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("armada did not become ready within %s", timeout)
}

func runTestsuite() error {
	if os.Getenv("ARMADA_EXECUTOR_INGRESS_URL") == "" {
		os.Setenv("ARMADA_EXECUTOR_INGRESS_URL", "http://localhost")
	}
	if os.Getenv("ARMADA_EXECUTOR_INGRESS_PORT") == "" {
		os.Setenv("ARMADA_EXECUTOR_INGRESS_PORT", "5001")
	}
	return sh.RunV("go", "run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*,testsuite/testcases/categorization/*",
		"--junit", "junit.xml",
		"--config", armadactlConfig,
	)
}
