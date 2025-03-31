package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
)

const (
	javaBuilderImageName = "armada-java-client-builder"
	dockerfilePath       = "./build/java-client/Dockerfile"
	buildDir             = "/build"
)

// BuildJava Build armada Java client.
func BuildJava() error {
	mg.Deps(BootstrapProto)

	buildConfig, err := getBuildConfig()
	if err != nil {
		return err
	}

	err = dockerBuildImage(NewDockerBuildConfig(buildConfig.JavaBuilderBaseImage),
		javaBuilderImageName, dockerfilePath)
	if err != nil {
		return err
	}

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	return dockerRun("run",
		"--rm",
		"-v", fmt.Sprintf("%s:%s", wd, buildDir),
		"-w", buildDir,
		javaBuilderImageName,
	)
}
