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
)

func getImagesUsedInTestsOrControllers() []string {
	return []string{
		"nginx:1.27.0", // Used by ingress-controller
		"alpine:3.20.0",
		"bitnamilegacy/kubectl:1.30",
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
	err = kindRun("create", "cluster", "--config", "e2e/setup/kind.yaml")
	if err != nil {
		return err
	}
	if err := kindWriteKubeConfig(); err != nil {
		return err
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
		"e2e/setup/ingress-nginx.yaml",
		"e2e/setup/priorityclasses.yaml",
		"e2e/setup/namespace-with-anonymous-user.yaml",
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
	// 1) Wait for nodes.
	if err := kubectlRun(
		"wait",
		"--for=condition=Ready", "node",
		"--all",
		"--timeout=10m",
		"--context", "kind-armada-test",
	); err != nil {
		return err
	}

	// 2) Wait for ingress workload.
	if err := kubectlRun(
		"-n", "ingress-nginx",
		"wait",
		"--for=condition=Available",
		"deployment/ingress-nginx-controller",
		"--timeout=10m",
		"--context", "kind-armada-test",
	); err != nil {
		// Fallback to daemonset form.
		if err2 := kubectlRun(
			"-n", "ingress-nginx",
			"rollout", "status",
			"daemonset/ingress-nginx-controller",
			"--timeout=10m",
			"--context", "kind-armada-test",
		); err2 != nil {
			return err
		}
	}

	// 3) Wait for controller pod readiness; if it times out, dump diagnostics.
	err := kubectlRun(
		"-n", "ingress-nginx",
		"wait",
		"--for=condition=ready", "pod",
		"--selector=app.kubernetes.io/component=controller",
		"--timeout=10m",
		"--context", "kind-armada-test",
	)
	if err == nil {
		return nil
	}

	// Diagnostics: print current pod status + describe + logs + recent events.
	fmt.Println("\n[ingress-nginx diagnostics]")

	// Pod list
	if out, e := sh.Output(kubectlBinary(),
		"--context", "kind-armada-test",
		"-n", "ingress-nginx",
		"get", "pods", "-o", "wide",
	); e == nil {
		fmt.Println("\n== kubectl get pods -n ingress-nginx -o wide ==")
		fmt.Println(out)
	}

	// Pick the controller pod name (first match)
	podName := ""
	if out, e := sh.Output(kubectlBinary(),
		"--context", "kind-armada-test",
		"-n", "ingress-nginx",
		"get", "pods",
		"-l", "app.kubernetes.io/component=controller",
		"-o", "jsonpath={.items[0].metadata.name}",
	); e == nil {
		podName = strings.TrimSpace(out)
	}

	if podName != "" {
		if out, e := sh.Output(kubectlBinary(),
			"--context", "kind-armada-test",
			"-n", "ingress-nginx",
			"describe", "pod", podName,
		); e == nil {
			fmt.Printf("\n== kubectl describe pod %s -n ingress-nginx ==\n", podName)
			fmt.Println(out)
		}

		if out, e := sh.Output(kubectlBinary(),
			"--context", "kind-armada-test",
			"-n", "ingress-nginx",
			"logs", podName,
			"--all-containers=true",
			"--tail=200",
		); e == nil {
			fmt.Printf("\n== kubectl logs %s -n ingress-nginx (all containers, tail 200) ==\n", podName)
			fmt.Println(out)
		}
	}

	// Events (recent)
	if out, e := sh.Output(kubectlBinary(),
		"--context", "kind-armada-test",
		"-n", "ingress-nginx",
		"get", "events",
		"--sort-by=.lastTimestamp",
	); e == nil {
		fmt.Println("\n== kubectl get events -n ingress-nginx --sort-by=.lastTimestamp ==")
		fmt.Println(out)
	}

	return err
}

func kindTeardown() error {
	return kindRun("delete", "cluster", "--name", KIND_NAME)
}