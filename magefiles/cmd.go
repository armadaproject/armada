package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/magefile/mage/sh"
)

var (
	GOPATH       string
	DockerGopath string
	Platform     string
	Host_arch    string
)

func dockerGoPath() {
	var err error
	GOPATH, err = sh.Output("go", "env", "GOPATH")
	if err != nil || GOPATH == "" {
		GOPATH = ".go"
	}
	DockerGopath = GOPATH
}

func platformGet() error {
	var err error
	Platform, err = sh.Output("uname", "-s")
	if err != nil {
		return err
	}
	Host_arch, err = sh.Output("uname", "-m")
	if err != nil {
		return err
	}
	return nil
}

func dockerRunAsUser() (string, error) {
	if err := platformGet(); err != nil {
		return "", fmt.Errorf("unable to get platform information: %v", err)
	}

	DockerRunAsUser := os.Getenv("DOCKER_RUN_AS_USER")
	if DockerRunAsUser == "" && Platform != "windows32" {
		userId, err := sh.Output("id", "-u")
		if err != nil {
			return "", fmt.Errorf("unable to get user id: %v", err)
		}
		groupId, err := sh.Output("id", "-g")
		if err != nil {
			return "", fmt.Errorf("unable to get group id: %v", err)
		}
		DockerRunAsUser = userId + ":" + groupId
	}
	return DockerRunAsUser, nil
}

func dockerNet() (string, error) {
	platform, err := sh.Output("uname", "-s")
	if err != nil {
		return "", fmt.Errorf("unable to get platform information: %v", err)
	} else if platform == "Darwin" {
		return "", nil
	}
	return "--network=host", nil
}

func dockerGopathDir() (string, error) {
	DockerGopath, err := sh.Output("go", "env", "GOPATH")
	if err != nil || DockerGopath == "" {
		DockerGopath = ".go"
	}

	DockerGopathToks := strings.Split(DockerGopath, ":")
	if len(DockerGopathToks) == 0 {
		return "", fmt.Errorf("unable to parse DockerGopath: %s", DockerGopath)
	}

	return DockerGopathToks[0], nil
}

func go_CMD() ([]string, error) {
	dockerGoPath()
	DOCKER_RUN_AS_USER, err := dockerRunAsUser()
	if err != nil {
		return nil, err
	}
	DOCKER_NET, err := dockerNet()
	if err != nil {
		return nil, err
	}
	DOCKER_GOPATH_DIR, err := dockerGopathDir()
	if err != nil {
		return nil, err
	}

	return []string{
		"run",
		"--rm",
		"-u",
		DOCKER_RUN_AS_USER,
		"-v",
		"${PWD}:/go/src/armada",
		"-w",
		"/go/src/armada",
		DOCKER_NET,
		"-e",
		"GOPROXY",
		"-e",
		"GOPRIVATE",
		"-e",
		"GOCACHE=/go/cache",
		"-e",
		"INTEGRATION_ENABLED=true",
		"-e",
		"CGO_ENABLED=0",
		"-e",
		"GOOS=linux",
		"-e",
		"GARCH=amd64",
		"-v",
		fmt.Sprintf("%s:/go", DOCKER_GOPATH_DIR),
		"golang:1.20.2-buster",
	}, nil
}

func go_TEST_CMD() ([]string, error) {
	TESTS_IN_DOCKER := os.Getenv("TESTS_IN_DOCKER")
	if TESTS_IN_DOCKER == "true" {
		return go_CMD()
	} else {
		return []string{}, nil
	}
}

func dotnetCmd() []string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	dotnetcmd := []string{
		"run",
		"-v", fmt.Sprintf("%s:/go/src/armada", wd),
		"-w", "/go/src/armada",
	}

	if useSystemCerts {
		dotnetcmd = append(dotnetcmd, "-v", "${PWD}/build/ssl/certs/:/etc/ssl/certs", "-e", "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt")
	}

	dotnetcmd = append(dotnetcmd, defaultDotnetDockerImg)
	return dotnetcmd
}
