package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

var services = []string{"pulsar", "redis", "postgres"}

var componentsStr string = "server, lookout, lookoutingester, lookoutv2, lookoutingesterv2, executor, binoculars, eventingester, jobservice"

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

// Dependencies include pulsar, postgres (v1 and v2) as well as redis
func StartDependencies() error {
	if onArm() {
		os.Setenv("PULSAR_IMAGE", "richgross/pulsar:2.11.0")
	}

	// append "up", "-d" to the beginning of services
	servicesArg := append([]string{"compose", "up", "-d"}, services...)
	if err := dockerRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// Stops the dependencies
func StopDependencies() error {
	servicesArg := append([]string{"compose", "stop"}, services...)
	if err := dockerRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// Starts the Armada Components
func StartComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()

	componentsArg := append([]string{"compose", "-f", composeFile, "up", "-d"}, components...)
	if err := dockerRun(componentsArg...); err != nil {
		return err
	}

	return nil
}

// Stops the Armada Components
func StopComponents() error {
	composeFile := getComposeFile()
	components := getComponentsList()

	componentsArg := append([]string{"compose", "-f", composeFile, "stop"}, components...)
	if err := dockerRun(componentsArg...); err != nil {
		return err
	}

	return nil
}

// Repeatedly check logs until "alive": true is found.
// Timeout after 1 minute
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
				// Sleep for 1 second to allow Pulsar to fully start
				time.Sleep(1 * time.Second)
				fmt.Printf("\nPulsar took %d seconds to start!\n\n", seconds)
				return nil
			}
			seconds++
		}
	}
}

// Download Dependency Images using Docker
// Ensure there is no error returned so that CI doesn't fail.
func downloadDependencyImages() error {
	timeTaken := time.Now()
	_, err := exec.Command(dockerBinary(), "compose", "pull", "--ignore-pull-failures").CombinedOutput()
	if err != nil {
		return nil
	}
	fmt.Printf("Time to download images: %s\n\n", time.Since(timeTaken))
	return nil
}
