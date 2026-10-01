package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"
)

const (
	KIND_VERSION_CONSTRAINT = ">= 0.21.0"
	KIND_CONFIG_INTERNAL    = ".kube/internal/config"
	KIND_CONFIG_EXTERNAL    = ".kube/external/config"
	KIND_CONFIG_DIR         = "_local/kind/cluster"
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

// kindInitCluster creates one kind cluster (idempotent) and writes its external kubeconfig.
func kindInitCluster(name, kindConfigPath, kubeconfigPath string) error {
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
	// The anonymous-user namespace and its RBAC, needed when running without auth. Its
	// ingress-nginx service account subject is harmless on clusters without ingress-nginx.
	if err := kubectlRun("apply", "-f", "_local/kind/namespace.yaml", "--context", "kind-"+name); err != nil {
		return err
	}
	return kindWriteExternalKubeConfig(name, kubeconfigPath)
}

// kindWriteExternalKubeConfig writes the named cluster's external kubeconfig to kubeconfigPath.
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

// KIND_MULTI_CLUSTER_KUBECONFIG_DIR is where kindClustersFromDir writes each cluster's external
// kubeconfig, one file per config-file basename, unless overridden by a fixed kubeconfigPath.
// Consumers (e.g. a multi-cluster test scenario) must reference
// kubeconfigs at KIND_MULTI_CLUSTER_KUBECONFIG_DIR/<basename> to match.
const KIND_MULTI_CLUSTER_KUBECONFIG_DIR = ".kube/external/multicluster"

// kindConfigClusterName reads the top-level "name:" field out of a kind-cluster config YAML
// file.
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

// kindClustersFromDir provisions one kind cluster per *.yaml file in configDir, returning the
// created cluster names in the same order as the globbed config files. Each cluster's external
// kubeconfig goes to kubeconfigDir/<config-file-basename>, unless kubeconfigPath is set, in which
// case (only valid for a single-file configDir) that exact path is used instead - the default
// dev/CI cluster needs this, since other tooling (the executor's .run configs,
// _local/compose/full.yaml) depends on its kubeconfig living at one fixed path.
func kindClustersFromDir(configDir, kubeconfigDir, kubeconfigPath string) ([]string, error) {
	entries, err := filepath.Glob(filepath.Join(configDir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errors.Errorf("no *.yaml kind-cluster configs found in %s", configDir)
	}
	if kubeconfigPath != "" && len(entries) != 1 {
		return nil, errors.Errorf("expected exactly one *.yaml kind-cluster config in %s, found %d", configDir, len(entries))
	}
	names := make([]string, 0, len(entries))
	for _, configPath := range entries {
		clusterName, err := kindConfigClusterName(configPath)
		if err != nil {
			return nil, err
		}
		targetKubeconfigPath := kubeconfigPath
		if targetKubeconfigPath == "" {
			targetName := strings.TrimSuffix(filepath.Base(configPath), ".yaml")
			targetKubeconfigPath = filepath.Join(kubeconfigDir, targetName)
		}
		if err := kindInitCluster(clusterName, configPath, targetKubeconfigPath); err != nil {
			return nil, err
		}
		names = append(names, clusterName)
	}
	return names, nil
}

// kindTeardownClustersFromDir deletes one kind cluster per *.yaml file in configDir, parallel to
// kindClustersFromDir.
func kindTeardownClustersFromDir(configDir string) error {
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

func kindSetupExternalImages(buildConfig BuildConfig, images []string, clusterName string) error {
	for _, image := range images {
		image = remapDockerRegistryIfRequired(image, buildConfig.DockerRegistries)
		if err := dockerRun("pull", image); err != nil {
			return fmt.Errorf("error pulling image: %w", err)
		}

		err := kindRun("load", "docker-image", image, "--name", clusterName)
		if err != nil {
			return fmt.Errorf("error loading image to kind: %w", err)
		}
	}

	return nil
}

// kindSetup provisions every cluster in configDir. Only when isDefault also writes the internal
// kubeconfig and applies the extra dev/CI resources (ingress-nginx, namespace, preloaded images)
// - a multi-cluster config dir gets just the cluster + priorityclasses.
func kindSetup(configDir string, isDefault bool) (string, error) {
	kubeconfigPath := ""
	if isDefault {
		kubeconfigPath = KIND_CONFIG_EXTERNAL
	}
	names, err := kindClustersFromDir(configDir, KIND_MULTI_CLUSTER_KUBECONFIG_DIR, kubeconfigPath)
	if err != nil {
		return "", err
	}
	name := names[0]
	if !isDefault {
		return name, nil
	}

	if err := kindWriteInternalKubeConfig(name); err != nil {
		return "", err
	}

	buildConfig, err := getBuildConfig()
	if err != nil {
		return "", err
	}

	err = kindSetupExternalImages(buildConfig, getImagesUsedInTestsOrControllers(), name)
	if err != nil {
		return "", err
	}

	resources := []string{
		"_local/kind/ingress-nginx.yaml",
		"_local/kind/priorityclasses.yaml",
	}
	for _, f := range resources {
		images, err := imagesFromFile(f)
		if err != nil {
			return "", err
		}

		err = kindSetupExternalImages(buildConfig, images, name)
		if err != nil {
			return "", err
		}

		file, err := remapDockerImagesInKubernetesManifest(f, images, buildConfig)
		if err != nil {
			return "", err
		}

		err = kubectlRun("apply", "-f", file, "--context", "kind-"+name)
		if err != nil {
			return "", err
		}
	}

	return name, nil
}

// kindWriteInternalKubeConfig writes the named cluster's internal kubeconfig to
// KIND_CONFIG_INTERNAL. Needed by the executor to interact with the cluster from inside the
// compose network.
func kindWriteInternalKubeConfig(name string) error {
	out, err := kindOutput("get", "kubeconfig", "--internal", "--name", name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(KIND_CONFIG_INTERNAL), os.ModeDir|0o755); err != nil {
		return err
	}
	f, err := os.Create(KIND_CONFIG_INTERNAL)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(out)
	return err
}

// kindWaitUntilReady waits for the named cluster's ingress-nginx controller to be ready.
func kindWaitUntilReady(name string) error {
	return kubectlRun(
		"wait",
		"--namespace", "ingress-nginx",
		"--for=condition=ready", "pod",
		"--selector=app.kubernetes.io/component=controller",
		"--timeout=2m",
		"--context", "kind-"+name,
	)
}
