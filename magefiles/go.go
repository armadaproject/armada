package main

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const GO_VERSION_CONSTRAINT = ">= 1.25.0"

func goBinary() string {
	return binaryWithExt("go")
}

func goOutput(args ...string) (string, error) {
	return sh.Output(goBinary(), args...)
}

func goRun(args ...string) error {
	return sh.Run(goBinary(), args...)
}

func goRunWith(env map[string]string, args ...string) error {
	return sh.RunWith(env, goBinary(), args...)
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
	return constraintCheck(version, GO_VERSION_CONSTRAINT, "Go")
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
	// standard module will have 2 fields when present
	if len(fields) == 2 {
		return fields[1], nil
	}
	// replaced module will have 5 fields
	if len(fields) == 5 {
		return fields[4], nil
	}
	return "", errors.Errorf("unexpected go list output: %v", fields)
}
