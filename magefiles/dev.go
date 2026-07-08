package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
//
// profiles is a comma-separated list of tokens:
//   - "no-auth"       - the default profile
//   - "auth"          - enables OIDC (Keycloak); sets goreman profile to "auth" and uses the auth compose profile
//   - "fake-executor" - no Kubernetes needed; sets goreman profile to "fake-executor"
//   - "hot-cold"      - runs the hot-cold scheduler setup
//   - anything else   - forwarded as a docker-compose --profile flag for extra services
//
// The optional -dap flag selects the "-dap" procfile variant, which starts each component
// under dlv in headless DAP mode so an editor debugger can attach.
//
// Optional flags need mage 1.16+ and older mage versions silently hide this target.
// `go run github.com/magefile/mage@v1.17.2 dev:up ...` runs a known-good version
// (the same pin the CI workflows use).
//
// Examples:
//
//	mage dev:up no-auth                   # default
//	mage dev:up auth                      # OIDC
//	mage dev:up fake-executor -dap        # fake executor + dap procfile
//	mage dev:up auth,myservice            # auth + extra compose profile "myservice"
//	mage dev:up hot-cold                  # hot-cold scheduler setup
func (Dev) Up(profiles string, dap *bool) error {
	var (
		profile         = "no-auth"
		composeProfiles []string
	)

	for _, token := range strings.Split(profiles, ",") {
		token = strings.TrimSpace(token)
		if token == "" || token == "no-auth" {
			continue
		}
		switch token {
		case "auth", "fake-executor", "hot-cold":
			if profile != "no-auth" {
				fmt.Printf("warning: ignoring %q - profile already set to %q; only one of auth/fake-executor/hot-cold may be used\n", token, profile)
			} else {
				profile = token
			}
		default:
			composeProfiles = append(composeProfiles, token)
		}
	}

	isDAP := dap != nil

	debugSuffix := ""
	if isDAP {
		debugSuffix = "-dap"
	}
	procfile := "_local/procfiles/" + profile + debugSuffix + ".Procfile"
	if _, err := os.Stat(procfile); err != nil {
		return fmt.Errorf("unknown profile %q: %s not found", profile+debugSuffix, procfile)
	}

	if profile == "auth" {
		composeProfiles = append([]string{"auth"}, composeProfiles...)
	}

	mg.Deps(installGoreman)
	if err := devDepsUp(strings.Join(composeProfiles, ",")); err != nil {
		return err
	}
	initArgs := []string{initScript}
	if profile == "hot-cold" {
		initArgs = append(initArgs, "--hotCold")
	}
	if err := sh.RunV(initArgs[0], initArgs[1:]...); err != nil {
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

// Migrate runs the database init step on its own: it applies the scheduler and lookout
// migrations and (when a cluster is reachable) applies the Armada priority classes. The
// databases themselves are created by _local/compose/postgres-init.sql when the postgres
// container first initialises. This is the same _local/scripts/init.sh that dev:up runs,
// minus starting goreman, so you can (re)apply schema changes against already-running
// dependencies. Bring the dependencies up first with `mage dev:deps` if they are not already
// running.
func (Dev) Migrate() error {
	return sh.RunV(initScript)
}

// Down stops docker-compose dependencies. If a `dev:up` goreman is running, Ctrl+C it first.
//
// --profile auth brings the keycloak container into scope so `dev:up auth` doesn't leave it
// orphaned, and --remove-orphans clears containers from any profile that is no longer active.
func (Dev) Down() error {
	return sh.RunV("docker", "compose", "-f", stackComposeFile, "--profile", "auth", "--profile", "prometheus", "down", "--remove-orphans")
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
func devDepsUp(profiles string) error {
	args := []string{"compose", "-f", stackComposeFile}
	for _, p := range strings.Split(profiles, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			args = append(args, "--profile", p)
		}
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
