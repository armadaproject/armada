package main

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const GO_VERSION_CONSTRAINT = ">= 1.20.0"

func goBinary() string {
	return binaryWithExt("go")
}

func goOutput(args ...string) (string, error) {
	return sh.Output(goBinary(), args...)
}

func goRun(args ...string) error {
	return sh.Run(goBinary(), args...)
}

func goVersion() (*semver.Version, error) {
	output, err := goOutput("version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(strings.TrimPrefix(fields[2], "go"))
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

func goCheck() error {
	version, err := goVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(GO_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constraint %v", version, constraint)
	}
	return nil
}

func goEnv(name string) (string, error) {
	return goOutput("env", name)
}

func goModuleVersion(name string) (string, error) {
	out, err := goOutput("list", "-m", name)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return "", errors.Errorf("unexpected go list output: %v", fields)
	}
	return fields[1], nil
}
