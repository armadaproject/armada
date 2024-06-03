package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
)

// Build armada python client.
func BuildPython() error {
	mg.Deps(BootstrapProto)

	buildConfig, err := getBuildConfig()
	if err != nil {
		return err
	}

	err = dockerBuildImage(NewDockerBuildConfig(buildConfig.PythonBuilderBaseImage), "armada-python-client-builder", "./build/python-client/Dockerfile")
	if err != nil {
		return err
	}

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	return dockerRun("run",
		"--rm",
		"-v", fmt.Sprintf("%s/proto:/proto", wd),
		"-v", fmt.Sprintf("%s:/go/src/armada", wd),
		"-w", "/go/src/armada",
		"armada-python-client-builder", "./scripts/build-python-client.sh")
}
