package kwok

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	log "github.com/armadaproject/armada/internal/common/logging"
)

const ControllerImage = "registry.k8s.io/kwok/kwok:v0.7.0"

// buildConfigEnv names the mage build config, whose dockerRegistries also apply to the controller image.
const buildConfigEnv = "ARMADA_BUILD_CONFIG"

// controllerImage is ControllerImage with its registry replaced by the build config's dockerRegistries. An
// unreadable config is reported and ignored: the pull of the default image then says what is unreachable.
func controllerImage() string {
	registries, err := dockerRegistriesFromBuildConfig(os.Getenv(buildConfigEnv))
	if err != nil {
		log.Warnf("ignoring %s: %s", buildConfigEnv, err)
		return ControllerImage
	}
	image := remapRegistry(ControllerImage, registries)
	if image != ControllerImage {
		log.Infof("kwok-controller image %s is pulled as %s (dockerRegistries in %s)", ControllerImage, image, os.Getenv(buildConfigEnv))
	}
	return image
}

// dockerRegistriesFromBuildConfig reads dockerRegistries from the build config; no path is no config.
func dockerRegistriesFromBuildConfig(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var buildConfig struct {
		DockerRegistries map[string]string `json:"dockerRegistries"`
	}
	if err := json.Unmarshal(content, &buildConfig); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return buildConfig.DockerRegistries, nil
}

// remapRegistry replaces the registry prefix of image as the mage targets do; the longest matching prefix wins.
func remapRegistry(image string, registries map[string]string) string {
	best := ""
	for from := range registries {
		if strings.HasPrefix(image, from) && len(from) > len(best) {
			best = from
		}
	}
	if best == "" {
		return image
	}
	return registries[best] + strings.TrimPrefix(image, best)
}

// Baked into the binary so regatta does not depend on the working directory.
//
//go:embed stage-crd.yaml
var stageCRDYAML []byte

//go:embed stages.yaml
var stagesYAML []byte

// controllerName is the per-target container name.
func controllerName(targetName string) string {
	return "armada-regatta-kwok-controller-" + targetName
}

// controllerKubeconfigDir holds the kubeconfig mounted into the controller. It is in the user's cache directory, not
// the shared temp directory, because the file holds live credentials. The path is fixed so TeardownController can
// remove it, and the file must outlive RunController: deleting it early crashes the container.
func controllerKubeconfigDir(targetName string) (string, error) {
	if targetName == "" || targetName != filepath.Base(targetName) || targetName == "." || targetName == ".." {
		return "", fmt.Errorf("target name %q cannot be used in a file path", targetName)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("finding the user cache directory: %w", err)
	}
	return filepath.Join(cache, "regatta", "kwok-"+targetName), nil
}

// writeControllerKubeconfig writes the kubeconfig as a new 0600 file in a 0700 directory (not a symlink), replacing any earlier one.
func writeControllerKubeconfig(targetName string, content []byte) (string, error) {
	dir, err := controllerKubeconfigDir(targetName)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("creating %s: %w", dir, err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory (a symlink?); remove it and retry", dir)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", fmt.Errorf("restricting %s: %w", dir, err)
	}
	path := filepath.Join(dir, "kubeconfig")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("removing the previous kubeconfig: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("creating %s: %w", path, err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("writing %s: %w", path, err)
	}
	return path, file.Close()
}

// ApplyStageCRD installs the Stage CRD, which the out-of-cluster controller reads Stages from.
func ApplyStageCRD(ctx context.Context, kubeconfig string) error {
	return kubectlApply(ctx, kubeconfig, stageCRDYAML)
}

const stageCRDName = "stages.kwok.x-k8s.io"

// WaitForStageCRD blocks until the API server serves the Stage CRD: applying it returns before it is served, and the
// controller exits at startup if it is not.
func WaitForStageCRD(ctx context.Context, kubeconfig string) error {
	args := []string{"wait", "--for=condition=established", "crd/" + stageCRDName, "--timeout=60s"}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	if out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("waiting for the Stage CRD to be established: %w: %s", err, out)
	}
	return nil
}

// ApplyStages applies content, the target's Stage set (see ResolveStagesYAML), to the cluster.
func ApplyStages(ctx context.Context, kubeconfig string, content []byte) error {
	return kubectlApply(ctx, kubeconfig, content)
}

// ResolveStagesYAML returns stagesPath's contents, or the embedded default. An incomplete custom set only logs a warning.
func ResolveStagesYAML(stagesPath string) ([]byte, error) {
	if stagesPath == "" {
		return stagesYAML, nil
	}
	content, err := os.ReadFile(stagesPath)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", stagesPath, err)
	}
	warnIfStagesIncomplete(stagesPath, content)
	return content, nil
}

// kubectlApply pipes content to kubectl against the kubeconfig's current context (there is one kubeconfig per target).
func kubectlApply(ctx context.Context, kubeconfig string, content []byte) error {
	args := []string{"apply", "-f", "-"}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = bytes.NewReader(content)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply -f -: %w: %s", err, out)
	}
	return nil
}

// RunController starts the kwok-controller container for nodes carrying the fake-node annotation, and does nothing if
// it is already running. internalAPIServerAddress is the API server's address as seen from the container; kind
// clusters also join the "kind" docker network.
func RunController(ctx context.Context, kubeconfigPath, internalAPIServerAddress, targetName string, kind bool) error {
	name := controllerName(targetName)

	out, err := exec.CommandContext(ctx, "docker", "ps",
		"--filter", "name=^/"+name+"$",
		"--format", "{{.Names}}",
	).Output()
	if err != nil {
		return fmt.Errorf("checking for existing kwok-controller: %w", err)
	}
	if strings.TrimSpace(string(out)) == name {
		return nil
	}

	if internalAPIServerAddress == "" {
		return fmt.Errorf("target %q: cluster.internalApiServerAddress is required to start the kwok-controller container", targetName)
	}
	internalKubeconfig, err := buildInternalKubeconfig(ctx, kubeconfigPath, internalAPIServerAddress)
	if err != nil {
		return fmt.Errorf("building internal kubeconfig: %w", err)
	}
	internalKubeconfigPath, err := writeControllerKubeconfig(targetName, internalKubeconfig)
	if err != nil {
		return fmt.Errorf("writing internal kubeconfig: %w", err)
	}

	// Remove a stopped controller of the same name (stopped containers are kept, see below).
	_ = exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()

	// No --rm: a controller that exits at startup leaves its logs for waitForControllerUp.
	args := []string{"run", "-d", "--name", name}
	if kind {
		args = append(args, "--network", "kind")
	}
	args = append(args,
		"-v", internalKubeconfigPath+":/kubeconfig:ro",
		controllerImage(),
		"--kubeconfig=/kubeconfig",
		"--manage-all-nodes=false",
		"--manage-nodes-with-annotation-selector="+NodeAnnotation+"="+NodeAnnotationOK,
		// Without this the controller ignores Stage objects and our pod-complete-armada stage never fires.
		"--enable-crds=Stage",
	)
	cmd := exec.CommandContext(ctx, "docker", args...)
	runOut, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("starting kwok-controller: %w: %s", err, runOut)
	}
	return waitForControllerUp(ctx, name)
}

const (
	// controllerStableFor is how long the container must stay up to count as started: `docker run -d` succeeds even if
	// the process dies a moment later.
	controllerStableFor = 3 * time.Second
	controllerPoll      = 500 * time.Millisecond
)

// waitForControllerUp fails, with the exit code and log tail, if the container stopped within controllerStableFor.
func waitForControllerUp(ctx context.Context, name string) error {
	deadline := time.Now().Add(controllerStableFor)
	for {
		out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", name).Output()
		if err != nil {
			return fmt.Errorf("inspecting kwok-controller %s: %w", name, err)
		}
		running, exitCode, err := parseContainerState(string(out))
		if err != nil {
			return err
		}
		if !running {
			logs, _ := exec.CommandContext(ctx, "docker", "logs", "--tail", "20", name).CombinedOutput()
			return fmt.Errorf("kwok-controller %s exited at startup (exit code %d): %s", name, exitCode, strings.TrimSpace(string(logs)))
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(controllerPoll):
		}
	}
}

// parseContainerState reads the output of `docker inspect -f "{{.State.Running}} {{.State.ExitCode}}"`.
func parseContainerState(out string) (running bool, exitCode int, err error) {
	fields := strings.Fields(out)
	if len(fields) != 2 {
		return false, 0, fmt.Errorf("unexpected docker inspect output %q", strings.TrimSpace(out))
	}
	running, err = strconv.ParseBool(fields[0])
	if err != nil {
		return false, 0, fmt.Errorf("unexpected docker inspect output %q: %w", strings.TrimSpace(out), err)
	}
	exitCode, err = strconv.Atoi(fields[1])
	if err != nil {
		return false, 0, fmt.Errorf("unexpected docker inspect output %q: %w", strings.TrimSpace(out), err)
	}
	return running, exitCode, nil
}

// stageDoc is the part of a Stage needed to tell which pod transition it handles.
type stageDoc struct {
	Kind string `yaml:"kind"`
	Spec struct {
		ResourceRef struct {
			Kind string `yaml:"kind"`
		} `yaml:"resourceRef"`
		Selector struct {
			MatchExpressions []struct {
				Key string `yaml:"key"`
			} `yaml:"matchExpressions"`
		} `yaml:"selector"`
	} `yaml:"spec"`
}

// requiredPodTransitions are substrings expected in the selector keys of some Pod Stage, one per lifecycle
// transition. A loose match, since it only drives a warning.
var requiredPodTransitions = map[string]string{
	"podIP":             "pod becoming Ready",
	"status.phase":      "pod completing",
	"deletionTimestamp": "pod deletion",
}

// warnIfStagesIncomplete warns, and never fails, when the Pod stages seem to leave a lifecycle transition unhandled: a
// custom set may do so on purpose.
func warnIfStagesIncomplete(stagesPath string, content []byte) {
	seen := map[string]bool{}
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	for {
		var doc stageDoc
		err := decoder.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warnf("%s: could not parse as YAML to check Pod-stage completeness: %s", stagesPath, err)
			return
		}
		if doc.Kind != "Stage" || doc.Spec.ResourceRef.Kind != "Pod" {
			continue
		}
		for _, expr := range doc.Spec.Selector.MatchExpressions {
			for substr := range requiredPodTransitions {
				if strings.Contains(expr.Key, substr) {
					seen[substr] = true
				}
			}
		}
	}
	for substr, description := range requiredPodTransitions {
		if !seen[substr] {
			log.Warnf("%s: no Pod-kind Stage selector appears to handle %s (looking for %q) - pods may get stuck at this transition; if that's intentional, ignore this warning", stagesPath, description, substr)
		}
	}
}

// KubeconfigServerAddress is the server address of the kubeconfig's current context.
func KubeconfigServerAddress(kubeconfigPath string) (string, error) {
	rawConfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return "", fmt.Errorf("loading %s: %w", kubeconfigPath, err)
	}
	context, ok := rawConfig.Contexts[rawConfig.CurrentContext]
	if !ok {
		return "", fmt.Errorf("%s: current-context %q not found", kubeconfigPath, rawConfig.CurrentContext)
	}
	cluster, ok := rawConfig.Clusters[context.Cluster]
	if !ok {
		return "", fmt.Errorf("%s: cluster %q not found", kubeconfigPath, context.Cluster)
	}
	return cluster.Server, nil
}

// buildInternalKubeconfig returns the kubeconfig's current context with the server replaced by
// internalAPIServerAddress, as the container sees it. TLSServerName is kept. An exec credential plugin is run here, on
// the host, and replaced by what it returns, because the container has neither the plugin nor its session; the
// credential can expire during a long run.
func buildInternalKubeconfig(ctx context.Context, kubeconfigPath, internalAPIServerAddress string) ([]byte, error) {
	rawConfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("loading %s: %w", kubeconfigPath, err)
	}
	// Keep the current context only, and embed certificate files: the container sees just this file.
	if err := clientcmdapi.MinifyConfig(rawConfig); err != nil {
		return nil, fmt.Errorf("%s: %w", kubeconfigPath, err)
	}
	if err := clientcmd.ResolveLocalPaths(rawConfig); err != nil {
		return nil, fmt.Errorf("%s: resolving file paths: %w", kubeconfigPath, err)
	}
	if err := clientcmdapi.FlattenConfig(rawConfig); err != nil {
		return nil, fmt.Errorf("%s: embedding certificate files: %w", kubeconfigPath, err)
	}
	context_, ok := rawConfig.Contexts[rawConfig.CurrentContext]
	if !ok {
		return nil, fmt.Errorf("%s: current-context %q not found", kubeconfigPath, rawConfig.CurrentContext)
	}
	cluster, ok := rawConfig.Clusters[context_.Cluster]
	if !ok {
		return nil, fmt.Errorf("%s: cluster %q not found", kubeconfigPath, context_.Cluster)
	}
	cluster.Server = internalAPIServerAddress

	authInfo, ok := rawConfig.AuthInfos[context_.AuthInfo]
	if !ok {
		return nil, fmt.Errorf("%s: user %q not found", kubeconfigPath, context_.AuthInfo)
	}
	if authInfo.Exec != nil {
		if err := resolveExecCredential(ctx, authInfo); err != nil {
			return nil, fmt.Errorf("resolving exec credential plugin: %w", err)
		}
	}

	return clientcmd.Write(*rawConfig)
}

// resolveExecCredential runs the exec credential plugin and replaces it with the static token or client certificate
// it returns.
func resolveExecCredential(ctx context.Context, authInfo *clientcmdapi.AuthInfo) error {
	execCfg := authInfo.Exec
	cmd := execCommandContext(ctx, execCfg)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("running %s: %w", execCfg.Command, err)
	}

	var cred v1beta1.ExecCredential
	if err := json.Unmarshal(out, &cred); err != nil {
		return fmt.Errorf("parsing ExecCredential output: %w", err)
	}
	if cred.Status == nil {
		return fmt.Errorf("%s: ExecCredential response had no status", execCfg.Command)
	}
	hasToken := cred.Status.Token != ""
	hasCertificate := cred.Status.ClientCertificateData != "" && cred.Status.ClientKeyData != ""
	if !hasToken && !hasCertificate {
		return fmt.Errorf("%s: ExecCredential response had neither a token nor a client certificate/key", execCfg.Command)
	}

	if hasToken {
		authInfo.Token = cred.Status.Token
	}
	if hasCertificate {
		authInfo.ClientCertificateData = []byte(cred.Status.ClientCertificateData)
		authInfo.ClientKeyData = []byte(cred.Status.ClientKeyData)
	}
	if hasToken && !cred.Status.ExpirationTimestamp.IsZero() {
		log.Warnf("%s: the kwok-controller will authenticate with a token that expires at %s; once it does the controller can no longer update its fake nodes, so a longer run needs a longer-lived credential",
			execCfg.Command, cred.Status.ExpirationTimestamp.Format(time.RFC3339))
	}
	authInfo.Exec = nil
	return nil
}

// execCommandContext builds the plugin command; its env entries are added to the host's environment.
func execCommandContext(ctx context.Context, execCfg *clientcmdapi.ExecConfig) *exec.Cmd {
	cmd := exec.CommandContext(ctx, execCfg.Command, execCfg.Args...)
	cmd.Env = os.Environ()
	for _, e := range execCfg.Env {
		cmd.Env = append(cmd.Env, e.Name+"="+e.Value)
	}
	return cmd
}

// TeardownController removes the controller container and its kubeconfig. The fake nodes live in the cluster, see
// DeleteFakeNodes.
func TeardownController(ctx context.Context, targetName string) error {
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", controllerName(targetName)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("stopping kwok-controller: %w: %s", err, out)
	}
	dir, err := controllerKubeconfigDir(targetName)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("removing internal kubeconfig: %w", err)
	}
	return nil
}
