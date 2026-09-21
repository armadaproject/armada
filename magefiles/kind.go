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
	"sigs.k8s.io/yaml"
)

const (
	KIND_VERSION_CONSTRAINT = ">= 0.21.0"
	KIND_CONFIG_INTERNAL    = ".kube/internal/config"
	KIND_CONFIG_EXTERNAL    = ".kube/external/config"
	KIND_NAME               = "armada-test"

	// KIND_NAME_REGATTA_1/_2 back two clusters dedicated to regatta's own quickstart. Bare
	// control-plane-only - regatta needs nothing beyond a reachable API server to host fake
	// Node objects.
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

// kindInitRegattaCluster creates one of the two dedicated regatta clusters (idempotent) and
// writes its external kubeconfig - parameterized over name/kind-config/kubeconfig path so both
// clusters share one implementation.
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
	if err := kindInitRegattaCluster(KIND_NAME_REGATTA_1, "cmd/regatta/config/armada/kind/regatta-1.yaml", KIND_CONFIG_EXTERNAL_REGATTA_1); err != nil {
		return err
	}
	return kindInitRegattaCluster(KIND_NAME_REGATTA_2, "cmd/regatta/config/armada/kind/regatta-2.yaml", KIND_CONFIG_EXTERNAL_REGATTA_2)
}

// REGATTA_RENDERED_KUBECONFIG_DIR is where kindInitRegattaClustersFromDir writes each rendered
// cluster's external kubeconfig, one file per target name. A scenario file rendered via
// `regatta render` must point its cluster.kubeconfig fields at
// REGATTA_RENDERED_KUBECONFIG_DIR/<target-name> to match - this is a documented convention (see
// cmd/regatta/README.md), not something mage or regatta enforce for each other.
const REGATTA_RENDERED_KUBECONFIG_DIR = ".kube/external/regatta"

// kindConfigClusterName reads the top-level "name:" field out of a kind-cluster config YAML
// file, so a directory of regatta-render output (named by execution-target name, not by a
// KIND_NAME_REGATTA_N constant) can be provisioned without mage needing to know target names in
// advance.
func kindConfigClusterName(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var doc struct {
		Name string `json:"name"`
	}
	if err := yaml.Unmarshal(content, &doc); err != nil {
		return "", fmt.Errorf("parsing %s: %w", path, err)
	}
	if doc.Name == "" {
		return "", fmt.Errorf("%s: missing top-level \"name\" field", path)
	}
	return doc.Name, nil
}

// kindInitRegattaClustersFromDir provisions one kind cluster per *.yaml file in configDir,
// mirroring kindInitRegattaClusters but for an arbitrary N of rendered configs rather than the
// two hardcoded quickstart clusters. Each cluster's external kubeconfig is written to
// REGATTA_RENDERED_KUBECONFIG_DIR/<target-name>, where <target-name> is the config file's own
// basename (without extension) - regatta render names its output files by target name, so this
// recovers the target name without needing a separate manifest.
func kindInitRegattaClustersFromDir(configDir string) error {
	entries, err := filepath.Glob(filepath.Join(configDir, "*.yaml"))
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return errors.Errorf("no *.yaml kind-cluster configs found in %s - run `regatta render` first", configDir)
	}
	for _, configPath := range entries {
		clusterName, err := kindConfigClusterName(configPath)
		if err != nil {
			return err
		}
		targetName := strings.TrimSuffix(filepath.Base(configPath), ".yaml")
		kubeconfigPath := filepath.Join(REGATTA_RENDERED_KUBECONFIG_DIR, targetName)
		if err := kindInitRegattaCluster(clusterName, configPath, kubeconfigPath); err != nil {
			return err
		}
	}
	return nil
}

// kindTeardownRegattaClustersFromDir deletes one kind cluster per *.yaml file in configDir,
// parallel to kindInitRegattaClustersFromDir.
func kindTeardownRegattaClustersFromDir(configDir string) error {
	entries, err := filepath.Glob(filepath.Join(configDir, "*.yaml"))
	if err != nil {
		return err
	}
	for _, configPath := range entries {
		clusterName, err := kindConfigClusterName(configPath)
		if err != nil {
			return err
		}
		if err := kindRun("delete", "cluster", "--name", clusterName); err != nil {
			return err
		}
	}
	return nil
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
