package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const (
	PROTOC_VERSION_CONSTRAINT = ">= 3.17.3"
	PROTOC_VERSION_DOWNLOAD   = "3.17.3"
)

func protocBinary() string {
	return binaryWithExt("./protoc/bin/protoc")
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

func protocInstall() error {
	if ok, err := exists(protocBinary()); ok || err != nil {
		return err
	}

	archOs, err := protocArchOs()
	if err != nil {
		return err
	}
	url := fmt.Sprintf(
		"https://github.com/protocolbuffers/protobuf/releases/download/v%s/protoc-%s-%s.zip",
		PROTOC_VERSION_DOWNLOAD, PROTOC_VERSION_DOWNLOAD, archOs,
	)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.ContentLength < 1000 {
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.Errorf("failed to download protoc: unexpected response '%s'", string(contents))
	}

	f, err := os.Create("protoc.zip")
	if err != nil {
		return err
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	defer os.RemoveAll("protoc.zip")
	if err := unzip("protoc.zip", "./protoc"); err != nil {
		return err
	}

	return nil
}

func protocArchOs() (string, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/amd64":
		return "osx-x86_64", nil
	case "darwin/arm64":
		return "osx-aarch_64", nil
	case "linux/386":
		return "linux-x86_32", nil
	case "linux/amd64":
		return "linux-x86_64", nil
	case "linux/arm64":
		return "linux-aarch_64", nil
	case "windows/386":
		return "win32", nil
	case "windows/amd64":
		return "win64", nil
	}
	return "", errors.Errorf("protoc not supported on %s/%s", runtime.GOOS, runtime.GOARCH)
}
