package main

import (
	"github.com/magefile/mage/mg"
)

// Build armada python client.
func BuildPython() error {
	mg.Deps(BootstrapProto)

	err := dockerRun("buildx", "build", "-o", "type=docker", "-t", "armada-python-client-builder", "-f", "./build/python-client/Dockerfile", ".")
	if err != nil {
		return err
	}

	return dockerRun("run", "--rm", "-v", "${PWD}/proto:/proto", "-v", "${PWD}:/go/src/armada", "-w", "/go/src/armada", "armada-python-client-builder", "./scripts/build-python-client.sh")
}
