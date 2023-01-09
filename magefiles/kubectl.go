package main

import (
	"encoding/json"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const KUBECTL_VERSION_CONSTRAINT = ">= 1.20.0"

func kubectlBinary() string {
	return binaryWithExt("kubectl")
}

func kubectlOutput(args ...string) (string, error) {
	return sh.Output(kubectlBinary(), args...)
}

func kubectlRun(args ...string) error {
	return sh.Run(kubectlBinary(), args...)
}

func kubectlVersion() (*semver.Version, error) {
	output, err := kubectlOutput("version", "--client=true", "--output=json")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	var obj struct {
		ClientVersion struct {
			GitVersion string `json:"gitVersion"`
		} `json:"clientVersion"`
	}
	err = json.Unmarshal([]byte(output), &obj)
	if err != nil {
		return nil, errors.Errorf("error parsing version cmd output: %v", err)
	}
	version, err := semver.NewVersion(obj.ClientVersion.GitVersion)
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

func kubectlCheck() error {
	version, err := kubectlVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(KUBECTL_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constaint %v", version, constraint)
	}
	return nil
}
