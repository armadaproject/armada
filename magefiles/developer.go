package main

import (
	"fmt"
	"os"
	"time"

	"github.com/magefile/mage/mg"
)

var services = []string{"pulsar", "postgres", "redis"}

var components = []string{"server", "lookout", "lookoutingester", "lookoutv2", "lookoutingesterv2", "executor", "binoculars", "eventingester", "jobservice"}

// Create a Local Armada Cluster
func LocalDev() error {
	mg.Deps(Kind)

	mg.Deps(StartDependencies)
	fmt.Println("Waiting for dependencies to start...")
	err := checkForPulsarRunning()
	mg.Deps(StartComponents)

	fmt.Println("Components are running!")
	fmt.Println("Run: `docker-compose logs -f` to see logs")
	return err
}

// Stop Local Armada Cluster
func LocalDevStop() {
	mg.Deps(StopComponents)
	mg.Deps(StopDependencies)
	mg.Deps(KindTeardown)
}

// Dependencies include pulsar, postgres (v1 and v2) as well as redis
func StartDependencies() error {
	// If user is on arm, export to env "PULSAR_IMAGE=kezhenxu94/pulsar"
	if onArm() {
		os.Setenv("PULSAR_IMAGE", "kezhenxu94/pulsar")
	}

	// append "up", "-d" to the beginning of services
	servicesArg := append([]string{"up", "-d"}, services...)
	if err := dockerComposeRun(servicesArg...); err != nil {
		return err
	}

	return nil
}

// StopDependencies stops the dependencies
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

func checkForPulsarRunning() error {
	if err := dockerComposeRun("exec", "pulsar", "bin/pulsar-admin", "tenants", "create", "test"); err == nil {
		return nil
	}

	time.Sleep(time.Second * 50)

	return nil
}
