package main

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const DOCKER_VERSION_CONSTRAINT = ">= 19.0.0"

func dockerBinary() string {
	return binaryWithExt("docker")
}

func dockerOutput(args ...string) (string, error) {
	return sh.Output(dockerBinary(), args...)
}

func dockerRun(args ...string) error {
	return sh.Run(dockerBinary(), args...)
}

func dockerVersion() (*semver.Version, error) {
	output, err := dockerOutput("--version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(strings.Trim(fields[2], ","))
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

func dockerCheck() error {
	version, err := dockerVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(DOCKER_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constaint %v", version, constraint)
	}
	return nil
}
