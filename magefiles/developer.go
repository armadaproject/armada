package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

var services = []string{"pulsar", "redis", "postgres"}

var componentsStr string = "server,lookout,lookoutingester,lookoutv2,lookoutingesterv2,executor,binoculars,eventingester,jobservice"

func getComposeFile() string {
	if os.Getenv("COMPOSE_FILE") != "" {
		return os.Getenv("COMPOSE_FILE")
	}
	return "docker-compose.yaml"
}

func getComponentsList() []string {
	if os.Getenv("ARMADA_COMPONENTS") != "" {
		return strings.Split(os.Getenv("ARMADA_COMPONENTS"), ",")
	}
	return strings.Split(componentsStr, ",")
}

// Dependencies include pulsar, postgres (v1 and v2) as well as redis.
func StartDependencies() error {
	// append "up", "-d" to the beginning of services
	servicesArg := append([]string{"compose", "up", "-d"}, services...)
	if err := dockerRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// Stops the dependencies.
func StopDependencies() error {
	servicesArg := append([]string{"compose", "stop"}, services...)
	if err := dockerRun(servicesArg...); err != nil {
		return err
	}

	servicesArg = append([]string{"compose", "rm", "-f"}, services...)
	if err := dockerRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// Starts the Armada Components. (Based on the ARMADA_COMPONENTS environment variable)
func StartComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()

	componentsArg := append([]string{"compose", "-f", composeFile, "up", "-d"}, components...)
	if err := dockerRun(componentsArg...); err != nil {
		return err
	}

	return nil
}

// Stops the Armada Components. (Based on the ARMADA_COMPONENTS environment variable)
func StopComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()

	// Adding the pulsar components here temporarily so that they can be stopped without
	// adding them to the full run (which is still on legacy scheduler)
	// TODO: remove this when pulsar backed scheduler is the default
	components = append(components, "server-pulsar", "executor-pulsar", "scheduler", "scheduleringester")

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
