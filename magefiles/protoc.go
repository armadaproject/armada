package main

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const PROTOC_VERSION_CONSTRAINT = ">= 3.17.3"

func protocBinary() string {
	return binaryWithExt("protoc")
}

func protocOutput(args ...string) (string, error) {
	return sh.Output(protocBinary(), args...)
}

func protocRun(args ...string) error {
	return sh.Run(protocBinary(), args...)
}

func protocVersion() (*semver.Version, error) {
	output, err := protocOutput("--version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 2 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(fields[1])
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

func protocCheck() error {
	version, err := protocVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(PROTOC_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constaint %v", version, constraint)
	}
	return nil
}
