package main

import semver "github.com/Masterminds/semver/v3"

func helmBinary() string {
	return binaryWithExt("helm")
}

func helmRun(args ...string) error {
	return nil
}

func helmVersion() (*semver.Version, error) {
	return &semver.Version{}, nil
}

func helmCheck() error {
	return nil
}
