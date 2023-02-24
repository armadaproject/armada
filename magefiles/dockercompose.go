package main

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const DOCKER_COMPOSE_VERSION_CONSTRAINT = ">= 2.0.0"

func dockerComposeBinary() string {
	return binaryWithExt("docker-compose")
}

func dockerComposeOutput(args ...string) (string, error) {
	return sh.Output(dockerComposeBinary(), args...)
}

func dockerComposeRun(args ...string) error {
	return sh.Run(dockerComposeBinary(), args...)
}

func dockerComposeVersion() (*semver.Version, error) {
	output, err := dockerComposeOutput("version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 4 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(fields[3])
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

func dockerComposeCheck() error {
	version, err := dockerComposeVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(DOCKER_COMPOSE_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constaint %v", version, constraint)
	}
	return nil
}
