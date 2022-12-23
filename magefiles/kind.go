package main

import (
	"os"
	"path/filepath"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const (
	KIND_VERSION_CONSTRAINT = ">= 0.14.0"
	KIND_CONFIG_INTERNAL    = ".kube/internal/config"
	KIND_CONFIG_EXTERNAL    = ".kube/external/config"
	KIND_NAME               = "armada-test"
)

func kindBinary() string {
	return binaryWithExt("kind")
}

func kindOutput(args ...string) (string, error) {
	return sh.Output(kindBinary(), args...)
}

func kindRun(args ...string) error {
	return sh.Run(kindBinary(), args...)
}

func kindVersion() (*semver.Version, error) {
	output, err := kindOutput("version")
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

func kindCheck() error {
	version, err := kindVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(KIND_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constaint %v", version, constraint)
	}
	return nil
}

func kindSetup() error {
	out, err := kindOutput("get", "clusters")
	if err != nil {
		return err
	}
	if strings.Contains(out, KIND_NAME) {
		return nil
	}
	err = kindRun("create", "cluster", "--config", "e2e/setup/kind.yaml")
	if err != nil {
		return err
	}
	if err := kindWriteKubeConfig(); err != nil {
		return err
	}

	// Images that need to be available in the Kind cluster,
	// e.g., images required for e2e tests.
	images := []string{
		"alpine:3.10",
		"nginx:1.21.6",
		"bitnami/kubectl:1.24.8",
		"registry.k8s.io/ingress-nginx/controller:v1.4.0",
		"registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20220916-gd32f8c343",
	}
	for _, image := range images {
		err := dockerRun("pull", image)
		if err != nil {
			return err
		}
	}
	for _, image := range images {
		err := kindRun("load", "docker-image", image, "--name", KIND_NAME)
		if err != nil {
			return err
		}
	}

	// Resources to create in the Kind cluster.
	resources := []string{
		"e2e/setup/ingress-nginx.yaml",
		"e2e/setup/priorityclasses.yaml",
		"e2e/setup/namespace-with-anonymous-user.yaml",
	}
	for _, f := range resources {
		err := kubectlRun("apply", "-f", f, "--context", "kind-armada-test")
		if err != nil {
			return err
		}
	}

	return nil
}

// Write kubeconfig to disk.
// Needed by the executor to interact with the cluster.
func kindWriteKubeConfig() error {
	out, err := kindOutput("get", "kubeconfig", "--name", KIND_NAME)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(KIND_CONFIG_EXTERNAL), os.ModeDir|0o755); err != nil {
		return err
	}
	if f, err := os.Create(KIND_CONFIG_EXTERNAL); err != nil {
		return err
	} else {
		defer f.Close()
		if _, err := f.WriteString(out); err != nil {
			return err
		}
	}

	out, err = kindOutput("get", "kubeconfig", "--internal", "--name", KIND_NAME)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(KIND_CONFIG_INTERNAL), os.ModeDir|0o755); err != nil {
		return err
	}
	if f, err := os.Create(KIND_CONFIG_INTERNAL); err != nil {
		return err
	} else {
		defer f.Close()
		if _, err := f.WriteString(out); err != nil {
			return err
		}
	}
	return nil
}

func kindWaitUntilReady() error {
	return kubectlRun(
		"wait",
		"--namespace", "ingress-nginx",
		"--for=condition=ready", "pod",
		"--selector=app.kubernetes.io/component=controller",
		"--timeout=2m",
	)
}

func kindTeardown() error {
	return kindRun("delete", "cluster", "--name", KIND_NAME)
}
