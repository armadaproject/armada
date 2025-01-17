package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
)

// Build armada Scala client.
func BuildScala() error {
	mg.Deps(BootstrapProto)

	buildConfig, err := getBuildConfig()
	if err != nil {
		return err
	}

	err = dockerBuildImage(NewDockerBuildConfig(buildConfig.ScalaBuilderBaseImage),
		"armada-scala-client-builder", "./build/scala-client/Dockerfile")
	if err != nil {
		return err
	}

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	return dockerRun("run",
		"-u", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--rm",
		"-v", fmt.Sprintf("%s/proto:/proto", wd),
		"-v", fmt.Sprintf("%s:/go/src/armada", wd),
		"-w", "/go/src/armada",
		"armada-scala-client-builder")
}
