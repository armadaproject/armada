package main

import "os"

var services = []string{"pulsar", "postgres", "redis"}

// Dependencies include pulsar, postgres (v1 and v2) as well as redis
func StartDependencies() error {
	// If user is on arm, export to env "PULSAR_IMAGE=kezhenxu94/pulsar"
	if onArm() {
		os.Setenv("PULSAR_IMAGE", "kezhenxu94/pulsar")
	}

	// Start services using docker-compose
	for _, service := range services {
		if err := dockerComposeRun("up", "-d", service); err != nil {
			return err
		}
	}

	return nil
}

// StopDependencies stops the dependencies
func StopDependencies() error {
	for _, service := range services {
		if err := dockerComposeRun("down", "-v", service); err != nil {
			return err
		}
	}

	return nil
}
