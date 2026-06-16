package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Dev namespace: single entry point for Armada local development.
//
// Dependencies (redis, postgres, pulsar, optionally keycloak) run as containers
// via _local/compose/stack.yaml. Armada components run as host processes via
// goreman, reading per-component configs in _local/<component>/config.yaml.
type Dev mg.Namespace

const (
	goremanPackage   = "github.com/mattn/goreman@v0.3.15"
	stackComposeFile = "_local/compose/stack.yaml"
	fullComposeFile  = "_local/compose/full.yaml"
	initScript       = "_local/scripts/init.sh"
)

// Up brings up dependencies and runs Armada components via goreman with the chosen profile.
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
	if err := devDepsUp(profile); err != nil {
		return err
	}
	if err := sh.RunV(initScript); err != nil {
		return err
	}
	if profile == "auth" {
		if err := waitForKeycloak(2 * time.Minute); err != nil {
			return err
		}
	}
	return sh.RunV(goremanBin(), "-f", procfile, "start")
}

// waitForKeycloak polls keycloak's armada realm endpoint until it serves a 200, so that
// components that initialise their OIDC client at startup don't crash on first dial.
func waitForKeycloak(timeout time.Duration) error {
	fmt.Println("Waiting for keycloak to become ready...")
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 3 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://localhost:8180/realms/armada")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				fmt.Println("Keycloak ready.")
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("keycloak did not become ready within %s", timeout)
}

// Deps brings up only the docker-compose dependencies (redis, postgres, pulsar).
// Useful when running goreman manually in another terminal.
func (Dev) Deps() error {
	return devDepsUp("")
}

// Down stops docker-compose dependencies. If a `dev:up` goreman is running, Ctrl+C it first.
func (Dev) Down() error {
	return sh.RunV("docker", "compose", "-f", stackComposeFile, "down")
}

// Full brings up the entire Armada stack in containers via _local/compose/full.yaml.
// Unlike dev:up (host-process goreman flow), every component runs as a container, with a
// real executor against a Kind cluster. This is what CI uses for integration tests.
//
// It builds the bundle images, sets up Kind (which writes the executor's kubeconfig to
// .kube/internal/config), then brings the stack up. Migrations run as compose services
// ordered ahead of the components, so no separate init step is needed.
func (Dev) Full() error {
	mg.Deps(mg.F(goreleaserMinimalRelease, "bundle", "lookout-bundle"), Kind)
	return sh.RunV("docker", "compose", "-f", fullComposeFile, "up", "-d", "--wait")
}

// FullDown stops the containerized full stack and tears down the Kind cluster.
func (Dev) FullDown() error {
	if err := sh.RunV("docker", "compose", "-f", fullComposeFile, "down", "-v"); err != nil {
		return err
	}
	return kindTeardown()
}

// devDepsUp brings the dependency stack up and waits for healthchecks. redis/postgres/pulsar
// have healthchecks so --wait blocks until they're ready. Keycloak (auth profile) has no
// healthcheck on purpose; waitForKeycloak handles its readiness before goreman starts.
func devDepsUp(profile string) error {
	args := []string{"compose", "-f", stackComposeFile}
	if profile == "auth" {
		args = append(args, "--profile", "auth")
	}
	args = append(args, "up", "-d", "--wait")
	return sh.RunV("docker", args...)
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
