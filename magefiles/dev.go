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
//   - "no-auth"             - the default profile
//   - "auth"                - enables OIDC (Keycloak); sets goreman profile to "auth" and uses the auth compose profile
//   - "fake-executor"       - no Kubernetes needed; sets goreman profile to "fake-executor"
//   - "fake-executor-regatta" - like "fake-executor", but without goreman's own armada-fakeexecutor
//     process; use this when `regatta run` (see cmd/regatta) will spawn and manage the fake
//     executor itself, to avoid two fake executors registering at once
//   - "auth-fake-executor"  - auth server/scheduler/lookout/binoculars plus the fake executor (no Kubernetes)
//   - "hot-cold"            - runs the hot-cold scheduler setup
//   - "regatta"             - like "no-auth", but the executor tolerates the kwok.x-k8s.io/node
//     taint so jobs can schedule onto KWOK-simulated fake nodes; starts the 2-cluster quickstart's
//     two executors (see cmd/regatta). When the "prometheus" compose profile is also active,
//     points it at cmd/regatta/config/armada/prometheus/two-cluster.yaml so both executors are
//     scraped instead of just one.
//   - "regatta-ten-cluster" - like "regatta", but starts the 10-cluster example's ten executors
//     instead (see cmd/regatta), and (with the "prometheus" compose profile) scrapes all ten via
//     cmd/regatta/config/armada/prometheus/ten-cluster.yaml
//   - anything else         - forwarded as a docker-compose --profile flag for extra services
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
//	mage dev:up regatta                   # no-auth + executor tolerates KWOK fake-node taint
//	mage dev:up regatta-ten-cluster        # regatta's 10-cluster example instead of the 2-cluster quickstart
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
		case "auth", "fake-executor", "fake-executor-regatta", "hot-cold", "auth-fake-executor", "regatta", "regatta-ten-cluster":
			if profile != "no-auth" {
				fmt.Printf("warning: ignoring %q - profile already set to %q; only one of auth/fake-executor/fake-executor-regatta/hot-cold/auth-fake-executor/regatta/regatta-ten-cluster may be used\n", token, profile)
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
	procfileDir := "_local/procfiles/"
	procfileName := profile
	if profile == "regatta" || profile == "fake-executor-regatta" || profile == "regatta-ten-cluster" {
		procfileDir = "cmd/regatta/config/armada/procfiles/"
		if profile == "regatta-ten-cluster" {
			procfileName = "ten-cluster"
		}
	}
	procfile := procfileDir + procfileName + debugSuffix + ".Procfile"
	if _, err := os.Stat(procfile); err != nil {
		return fmt.Errorf("unknown profile %q: %s not found", profile+debugSuffix, procfile)
	}

	if profile == "auth" || profile == "auth-fake-executor" {
		composeProfiles = append([]string{"auth"}, composeProfiles...)
	}

	if err := setPrometheusConfig(profile); err != nil {
		return err
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
	if profile == "auth" || profile == "auth-fake-executor" {
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

// setPrometheusConfig points the prometheus compose service (profile: prometheus, see
// _local/compose/stack.yaml) at a per-topology scrape config for the regatta profiles, since
// _local/prometheus.yml only scrapes a single executor target and regatta's 2/10-cluster
// topologies each run their own executor per cluster on its own metrics port (see
// cmd/regatta/config/armada/prometheus/two-cluster.yaml and ten-cluster.yaml). Every other
// profile leaves PROMETHEUS_CONFIG unset, so stack.yaml's
// "${PROMETHEUS_CONFIG:-../prometheus.yml}" volume mount falls back to the existing default.
//
// The path is resolved to absolute before being set: compose resolves relative bind-mount
// sources against the compose file's own directory (_local/compose/), not the caller's cwd, so a
// repo-root-relative path here would resolve to the wrong location.
func setPrometheusConfig(profile string) error {
	var relPath string
	switch profile {
	case "regatta":
		relPath = "cmd/regatta/config/armada/prometheus/two-cluster.yaml"
	case "regatta-ten-cluster":
		relPath = "cmd/regatta/config/armada/prometheus/ten-cluster.yaml"
	default:
		return nil
	}
	absPath, err := filepath.Abs(relPath)
	if err != nil {
		return fmt.Errorf("resolving prometheus config path %q: %w", relPath, err)
	}
	return os.Setenv("PROMETHEUS_CONFIG", absPath)
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
