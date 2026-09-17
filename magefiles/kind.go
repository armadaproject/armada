package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/magefile/mage/mg"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const (
	KIND_VERSION_CONSTRAINT = ">= 0.21.0"
	KIND_CONFIG_INTERNAL    = ".kube/internal/config"
	KIND_CONFIG_EXTERNAL    = ".kube/external/config"
	KIND_NAME               = "armada-test"

	// KIND_NAME_2/KIND_CONFIG_EXTERNAL_2 back a second, minimal kind cluster used only to
	// exercise regatta's multi-cluster execution targets. Armada's own components run via
	// goreman on the host rather than inside either kind cluster, so the second cluster needs
	// no ingress-nginx/priorityclass/namespace install - it only needs to exist and be
	// reachable for a kwok-type execution target to attach fake nodes to it.
	KIND_NAME_2            = "armada-test-2"
	KIND_CONFIG_EXTERNAL_2 = ".kube/external/config-2"

	// KIND_NAME_REGATTA_1/_2 back two clusters dedicated to regatta's own quickstart, separate
	// from KIND_NAME/KIND_NAME_2 above (which serve other, non-regatta local-dev workflows). Bare
	// control-plane-only, same shape as KIND_NAME_2 - regatta needs nothing beyond a reachable
	// API server to host fake Node objects.
	KIND_NAME_REGATTA_1            = "armada-regatta-1"
	KIND_NAME_REGATTA_2            = "armada-regatta-2"
	KIND_CONFIG_EXTERNAL_REGATTA_1 = ".kube/external/config-regatta-1"
	KIND_CONFIG_EXTERNAL_REGATTA_2 = ".kube/external/config-regatta-2"
)

func getImagesUsedInTestsOrControllers() []string {
	return []string{
		"nginx:1.27.0", // Used by ingress-controller
		"alpine:3.20.0",
		"bitnamilegacy/kubectl:1.33.4",
	}
}

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
	return constraintCheck(version, KIND_VERSION_CONSTRAINT, "kind")
}

func kindInitCluster() error {
	out, err := kindOutput("get", "clusters")
	if err != nil {
		return err
	}
	if strings.Contains(out, KIND_NAME) {
		return nil
	}
	err = kindRun("create", "cluster", "--config", "_local/kind/cluster.yaml")
	if err != nil {
		return err
	}
	if err := kindWriteKubeConfig(); err != nil {
		return err
	}
	return nil
}

// kindInitCluster2 creates the second, minimal kind cluster used to test regatta's multi-cluster
// execution targets, parallel to kindInitCluster rather than folded into it - this keeps
// kindInitCluster's own behavior/output paths for the primary cluster completely unchanged.
func kindInitCluster2() error {
	out, err := kindOutput("get", "clusters")
	if err != nil {
		return err
	}
	if strings.Contains(out, KIND_NAME_2) {
		return nil
	}
	err = kindRun("create", "cluster", "--config", "_local/kind/cluster-2.yaml")
	if err != nil {
		return err
	}
	return kindWriteKubeConfig2()
}

// kindWriteKubeConfig2 writes only the external kubeconfig for the second cluster - no goreman
// process needs the internal one, since nothing runs inside either kind cluster.
func kindWriteKubeConfig2() error {
	out, err := kindOutput("get", "kubeconfig", "--name", KIND_NAME_2)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(KIND_CONFIG_EXTERNAL_2), os.ModeDir|0o755); err != nil {
		return err
	}
	f, err := os.Create(KIND_CONFIG_EXTERNAL_2)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(out); err != nil {
		return err
	}
	return nil
}

// kindTeardown2 deletes the second kind cluster, independent of the primary armada-test cluster.
func kindTeardown2() error {
	return kindRun("delete", "cluster", "--name", KIND_NAME_2)
}

// kindInitRegattaCluster creates one of the two dedicated regatta clusters (idempotent, mirrors
// kindInitCluster2's shape) and writes its external kubeconfig - parameterized over
// name/kind-config/kubeconfig path so both clusters share one implementation.
func kindInitRegattaCluster(name, kindConfigPath, kubeconfigPath string) error {
	out, err := kindOutput("get", "clusters")
	if err != nil {
		return err
	}
	if strings.Contains(out, name) {
		return nil
	}
	if err := kindRun("create", "cluster", "--name", name, "--config", kindConfigPath); err != nil {
		return err
	}
	// The executor creates real pods with priorityClassName: armada-default/armada-preemptible
	// even though nothing else runs on this cluster - the API server still validates that
	// against real PriorityClass objects, so they must exist even on an otherwise-empty cluster.
	if err := kubectlRun("apply", "-f", "_local/kind/priorityclasses.yaml", "--context", "kind-"+name); err != nil {
		return err
	}
	return kindWriteExternalKubeConfig(name, kubeconfigPath)
}

// kindWriteExternalKubeConfig writes only the external kubeconfig for the named cluster - no
// goreman process needs an internal one, since nothing regatta-specific runs inside either kind
// cluster.
func kindWriteExternalKubeConfig(name, kubeconfigPath string) error {
	out, err := kindOutput("get", "kubeconfig", "--name", name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(kubeconfigPath), os.ModeDir|0o755); err != nil {
		return err
	}
	f, err := os.Create(kubeconfigPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(out)
	return err
}

// kindInitRegattaClusters creates both dedicated regatta clusters.
func kindInitRegattaClusters() error {
	if err := kindInitRegattaCluster(KIND_NAME_REGATTA_1, "_local/kind/regatta-1.yaml", KIND_CONFIG_EXTERNAL_REGATTA_1); err != nil {
		return err
	}
	return kindInitRegattaCluster(KIND_NAME_REGATTA_2, "_local/kind/regatta-2.yaml", KIND_CONFIG_EXTERNAL_REGATTA_2)
}

// kindTeardownRegattaClusters deletes both dedicated regatta clusters, independent of any other
// kind clusters.
func kindTeardownRegattaClusters() error {
	if err := kindRun("delete", "cluster", "--name", KIND_NAME_REGATTA_1); err != nil {
		return err
	}
	return kindRun("delete", "cluster", "--name", KIND_NAME_REGATTA_2)
}

func imagesFromFile(resourceYamlPath string) ([]string, error) {
	content, err := os.ReadFile(resourceYamlPath)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	re := regexp.MustCompile(`(?m)image:\s*([^\s]+)`)
	matches := re.FindAllStringSubmatch(string(content), -1)
	if matches == nil {
		return nil, nil
	}

	var images []string
	for _, match := range matches {
		if len(match) > 1 {
			images = append(images, match[1])
		}
	}

	return images, nil
}

func remapDockerRegistryIfRequired(image string, registries map[string]string) string {
	for registryFrom, registryTo := range registries {
		if strings.HasPrefix(image, registryFrom) {
			return registryTo + strings.TrimPrefix(image, registryFrom)
		}
	}
	return image
}

func remapDockerImagesInKubernetesManifest(filePath string, images []string, buildConfig BuildConfig) (string, error) {
	if buildConfig.DockerRegistries == nil {
		return filePath, nil
	}

	content, err := os.ReadFile(filePath)
	if err != nil {
		return filePath, fmt.Errorf("error reading manifest: %w", err)
	}

	replacedContent := ""
	for _, image := range images {
		targetImage := remapDockerRegistryIfRequired(image, buildConfig.DockerRegistries)
		if targetImage != image {
			if replacedContent == "" {
				replacedContent = string(content)
			}

			replacedContent = strings.ReplaceAll(replacedContent, image, targetImage)
		}
	}

	if replacedContent == "" {
		return filePath, nil
	}

	f, err := os.CreateTemp("", "")
	if err != nil {
		return filePath, fmt.Errorf("error creating temporary file: %w", err)
	}
	_, err = f.WriteString(replacedContent)
	if err != nil {
		return filePath, fmt.Errorf("error writing temporary file: %w", err)
	}
	return f.Name(), nil
}

func kindSetupExternalImages(buildConfig BuildConfig, images []string) error {
	for _, image := range images {
		image = remapDockerRegistryIfRequired(image, buildConfig.DockerRegistries)
		if err := dockerRun("pull", image); err != nil {
			return fmt.Errorf("error pulling image: %w", err)
		}

		err := kindRun("load", "docker-image", image, "--name", KIND_NAME)
		if err != nil {
			return fmt.Errorf("error loading image to kind: %w", err)
		}
	}

	return nil
}

func kindSetup() error {
	mg.Deps(kindInitCluster)

	buildConfig, err := getBuildConfig()
	if err != nil {
		return err
	}

	err = kindSetupExternalImages(buildConfig, getImagesUsedInTestsOrControllers())
	if err != nil {
		return err
	}

	resources := []string{
		"_local/kind/ingress-nginx.yaml",
		"_local/kind/priorityclasses.yaml",
		"_local/kind/namespace.yaml",
	}
	for _, f := range resources {
		images, err := imagesFromFile(f)
		if err != nil {
			return err
		}

		err = kindSetupExternalImages(buildConfig, images)
		if err != nil {
			return err
		}

		file, err := remapDockerImagesInKubernetesManifest(f, images, buildConfig)
		if err != nil {
			return err
		}

		err = kubectlRun("apply", "-f", file, "--context", "kind-armada-test")
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
		"--context", "kind-armada-test",
	)
}

func kindTeardown() error {
	return kindRun("delete", "cluster", "--name", KIND_NAME)
}
