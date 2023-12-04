package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
)

// Build armada python client.
func BuildPython() error {
	mg.Deps(BootstrapProto)

	err := dockerRun("buildx", "build", "-o", "type=docker", "-t", "armada-python-client-builder", "-f", "./build/python-client/Dockerfile", ".")
	if err != nil {
		return err
	}

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	return dockerRun(
		"run",
		"--rm",
		"-v", fmt.Sprintf("%s/proto:/proto", wd),
		"-v", fmt.Sprintf("%s:/go/src/armada", wd),
		"-w", "/go/src/armada",
		"armada-python-client-builder",
		"./scripts/build-python-client.sh",
	)
}
