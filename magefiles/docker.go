package main

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const DOCKER_VERSION_CONSTRAINT = ">= 20.10.10"
const DOCKER_COMPOSE_VERSION_CONSTRAINT = ">= 1.29.2"
const DOCKER_BUILDX_VERSION_CONSTRAINT = ">= 0.6.3"

func dockerBinary() string {
	return binaryWithExt("docker")
}
func dockerOutput(args ...string) (string, error) {
	return sh.Output(dockerBinary(), args...)
}

func dockerRun(args ...string) error {
	return sh.Run(dockerBinary(), args...)
}

func dockerBuildxVersion() (*semver.Version, error) {
	output, err := dockerOutput("buildx", "version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(fields[1])
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil

}

func dockerComposeVersion() (*semver.Version, error) {
	output, err := dockerOutput("compose", "version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(fields[3])
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
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

func constraintCheck(version *semver.Version, versionRequirement string) error {
	constraint, err := semver.NewConstraint(versionRequirement)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constaint %v", version, constraint)
	}
	return nil
}

func dockerComposeCheck() error {
	version, err := dockerComposeVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	return constraintCheck(version, DOCKER_COMPOSE_VERSION_CONSTRAINT)
}

func dockerBuildxCheck() error {
	version, err := dockerBuildxVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	return constraintCheck(version, DOCKER_BUILDX_VERSION_CONSTRAINT)
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
