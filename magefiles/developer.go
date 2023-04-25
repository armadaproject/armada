package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

var services = []string{"pulsar", "postgres", "redis"}

var components = []string{"server", "lookout", "lookoutingester", "lookoutv2", "lookoutingesterv2", "executor", "binoculars", "eventingester", "jobservice"}

// Dependencies include pulsar, postgres (v1 and v2) as well as redis
func StartDependencies() error {
	if onArm() {
		os.Setenv("PULSAR_IMAGE", "richgross/pulsar:2.11.0")
	}

	// append "up", "-d" to the beginning of services
	servicesArg := append([]string{"up", "-d"}, services...)
	if err := dockerComposeRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// Stops the dependencies
func StopDependencies() error {
	servicesArg := append([]string{"down", "-v"}, services...)
	if err := dockerComposeRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// Starts the Armada Components
func StartComponents() error {
	componentsArg := append([]string{"up", "-d"}, components...)
	if err := dockerComposeRun(componentsArg...); err != nil {
		return err
	}

	return nil
}

// Stops the Armada Components
func StopComponents() error {
	componentsArg := append([]string{"down", "-v"}, components...)
	if err := dockerComposeRun(componentsArg...); err != nil {
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
			out, err := dockerComposeOutput("logs", "pulsar")
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
