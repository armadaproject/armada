package main

import (
	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const SQLC_VERSION_CONSTRAINT = ">= v1.22.0"

func sqlcBinary() string {
	return binaryWithExt("sqlc")
}

func sqlcOutput(args ...string) (string, error) {
	return sh.Output(sqlcBinary(), args...)
}

func sqlcRun(args ...string) error {
	return sh.Run(sqlcBinary(), args...)
}

func sqlcVersion() (*semver.Version, error) {
	output, err := sqlcOutput("version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	version, err := semver.NewVersion(output)
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

func sqlcCheck() error {
	version, err := sqlcVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	return constraintCheck(version, SQLC_VERSION_CONSTRAINT, "sqlc")
}
