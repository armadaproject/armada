package main

import (
	"fmt"
	"os"
	"os/exec"
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
	"lookoutv2",
	"lookoutingesterv2",
	"lookoutv2-migration",
}

var allComponents = append(
	slices.Clone(defaultComponents),
	"jobservice",
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

// Dependencies include pulsar, postgres (v1 and v2) as well as redis.
func StartDependencies() error {
	if onArm() {
		os.Setenv("PULSAR_IMAGE", "richgross/pulsar:2.11.0")
	}

	command := append([]string{"compose", "up", "-d"}, dependencies...)
	return dockerRun(command...)
}

// Stops the dependencies.
func StopDependencies() error {
	command := append([]string{"compose", "stop"}, dependencies...)
	if err := dockerRun(command...); err != nil {
		return err
	}

	command = append([]string{"compose", "rm", "-f"}, dependencies...)
	return dockerRun(command...)
}

// Starts the Armada Components. (Based on the ARMADA_COMPONENTS environment variable)
func StartComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()
	if len(components) == 0 {
		components = defaultComponents
	}

	componentsArg := append([]string{"compose", "-f", composeFile, "up", "-d"}, components...)
	if err := dockerRun(componentsArg...); err != nil {
		return err
	}

	return nil
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

// Repeatedly check logs until Pulsar is ready.
func CheckForPulsarRunning() error {
	timeout := time.After(1 * time.Minute)
	tick := time.Tick(1 * time.Second)
	seconds := 0
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for Pulsar to start")
		case <-tick:
			out, err := dockerOutput("compose", "logs", "pulsar")
			if err != nil {
				return err
			}
			if strings.Contains(out, "alive") {
				// if seconds is less than 1, it means that pulsar had already started
				if seconds < 1 {
					fmt.Printf("\nPulsar had already started!\n\n")
					return nil
				}

				fmt.Printf("\nPulsar took %d seconds to start!\n\n", seconds)
				return nil
			}
			seconds++
		}
	}
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
