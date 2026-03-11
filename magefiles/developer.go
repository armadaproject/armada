package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

var dependencies = []string{
	"redis",
	"postgres",
	"pulsar",
}

var defaultComponents = []string{
	"server",
	"scheduler",
	"scheduler-migration",
	"scheduleringester",
	"executor",
	"binoculars",
	"eventingester",
	"lookout",
	"lookoutingester",
	"lookout-migration",
}

var allComponents = append(
	slices.Clone(defaultComponents),
	"airflow",
)

func getComposeFile() string {
	if os.Getenv("COMPOSE_FILE") != "" {
		return os.Getenv("COMPOSE_FILE")
	}
	return "docker-compose.yaml"
}

func getComponentsList() []string {
	if os.Getenv("ARMADA_COMPONENTS") == "" {
		return []string{}
	}
	return strings.Split(os.Getenv("ARMADA_COMPONENTS"), ",")
}

// Runs scheduler and lookout migrations
func RunMigrations() error {
	composeFile := getComposeFile()
	migrations := []string{
		"scheduler-migration",
		"lookout-migration",
	}
	command := append([]string{"compose", "-f", composeFile, "up", "-d"}, migrations...)
	return dockerRun(command...)
}

// Starts armada infrastructure dependencies
func StartDependencies() error {
	composeFile := getComposeFile()
	command := append([]string{"compose", "-f", composeFile, "up", "-d"}, dependencies...)
	return dockerRun(command...)
}

// Stops the dependencies.
func StopDependencies() error {
	composeFile := getComposeFile()

	command := append([]string{"compose", "-f", composeFile, "stop"}, dependencies...)
	if err := dockerRun(command...); err != nil {
		return err
	}

	command = append([]string{"compose", "-f", composeFile, "rm", "-f"}, dependencies...)
	return dockerRun(command...)
}

// Starts the Armada Components. (Based on the ARMADA_COMPONENTS environment variable)
func StartComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()
	if len(components) == 0 {
		components = defaultComponents
	}

	// Do NOT force KUBECONFIG here.
	// docker-compose.override.yaml is expected to mount the kubeconfig and set env vars.
	componentsArg := append([]string{"compose", "-f", composeFile, "up", "-d"}, components...)
	return dockerRun(componentsArg...)
}

func StopComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()
	if len(components) == 0 {
		components = allComponents
	}

	componentsArg := append([]string{"compose", "-f", composeFile, "stop"}, components...)
	if err := dockerRun(componentsArg...); err != nil {
		return err
	}

	componentsArg = append([]string{"compose", "-f", composeFile, "rm", "-f"}, components...)
	if err := dockerRun(componentsArg...); err != nil {
		return err
	}

	return nil
}

func CheckPulsarRunning() error {
	// Use pulsar-admin (present in the image) rather than curl (may not be).
	// Also avoid `docker compose run` loops; exec into the already-running container.
	return CheckDockerServiceReadyExec("pulsar", []string{
		`bin/pulsar-admin --admin-url http://localhost:8080 brokers list >/dev/null`,
	})
}

func CheckPostgresRunning() error {
	// Fixes the bug where this previously checked pulsar, and waits for actual readiness.
	return CheckDockerServiceReadyExec("postgres", []string{
		`pg_isready -h localhost -p 5432 >/dev/null`,
	})
}

func CheckServerRunning() error {
	return CheckDockerContainerRunning("server", "Starting http server listening on")
}

func CheckSchedulerRunning() error {
	return CheckDockerContainerRunning("scheduler", "Starting http server listening on")
}

func CheckExecutorRunning() error {
	return CheckDockerContainerRunning("executor", "Starting http server listening on")
}

func CheckSchedulerReady() error {
	return CheckDockerContainerRunning("scheduler", "Retrieved [1-9]+ executors")
}

// Repeatedly check logs until container is ready.
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
			out, err := dockerOutput("compose", "logs", containerName)
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

// Runs readiness commands via `docker compose exec` against the *running* service container.
// This is appropriate for dependencies (pulsar/postgres/redis) that should remain running.
func CheckDockerServiceReadyExec(serviceName string, shCommands []string) error {
	// Pulsar can legitimately take a while on cold starts.
	timeout := time.After(5 * time.Minute)
	tick := time.Tick(2 * time.Second)
	seconds := 0

	composeFile := getComposeFile()
	cmd := "set -e; " + strings.Join(shCommands, "; ")

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for %s to become ready", serviceName)
		case <-tick:
			out, err := exec.Command(
				dockerBinary(),
				"compose", "-f", composeFile,
				"exec", "-T", serviceName,
				"sh", "-lc", cmd,
			).CombinedOutput()

			if err == nil {
				if seconds < 1 {
					fmt.Printf("\n%s had already been ready!\n\n", serviceName)
				} else {
					fmt.Printf("\n%s became ready after %d seconds!\n\n", serviceName, seconds)
				}
				return nil
			}
			_ = out
			seconds += 2
		}
	}
}

func dockerRunWithEnv(env map[string]string, args ...string) error {
	cmd := exec.Command(dockerBinary(), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	return cmd.Run()
}

// Download Dependency Images for Docker Compose
func downloadDependencyImages() error {
	timeTaken := time.Now()
	_, err := exec.Command(dockerBinary(), "compose", "pull", "--ignore-pull-failures").CombinedOutput()
	if err != nil {
		return nil
	}
	fmt.Printf("Time to download images: %s\n\n", time.Since(timeTaken))
	return nil
}