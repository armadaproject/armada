package kwok

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/client-go/tools/clientcmd"
)

const ControllerImage = "registry.k8s.io/kwok/kwok:v0.7.0"

// controllerName derives a per-target docker container name so N simultaneous cluster targets
// don't collide on a single fixed container name.
func controllerName(targetName string) string {
	return "armada-regatta-kwok-controller-" + targetName
}

// controllerKubeconfigPath is where the internal kubeconfig bind-mounted into the
// kwok-controller container is written, namespaced per target. Fixed (rather than a random temp
// file) so TeardownController can find and remove it - it must outlive RunController's own
// return, since deleting it too early can race the container's own open() of the bind-mounted
// file on VM-backed docker runtimes (observed with OrbStack) and crash the container silently.
func controllerKubeconfigPath(targetName string) string {
	return filepath.Join(os.TempDir(), "regatta-kwok-kubeconfig-"+targetName)
}

// ApplyStageCRD installs the Stage CRD (stages.kwok.x-k8s.io) into the cluster. Out-of-cluster
// mode needs this as a real CRD - unlike the all-in-one image, which reads Stage definitions
// from a local -c stages.yaml config file at container-create time.
func ApplyStageCRD(ctx context.Context, kubeconfig, crdPath string) error {
	return kubectlApply(ctx, kubeconfig, crdPath)
}

// ApplyStages applies the Stage resources (node-heartbeat plus the Pod-kind stage set - see
// stages.yaml's header comment on why the Pod-kind set must stay complete) as real objects in
// the cluster.
func ApplyStages(ctx context.Context, kubeconfig, stagesPath string) error {
	return kubectlApply(ctx, kubeconfig, stagesPath)
}

// kubectlApply trusts the kubeconfig file's own current-context - regatta writes one kubeconfig
// file per execution target, so current-context is never ambiguous between targets.
func kubectlApply(ctx context.Context, kubeconfig, path string) error {
	args := []string{"apply", "-f", path}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply -f %s: %w: %s", path, err, out)
	}
	return nil
}

// RunController starts the standalone kwok-controller container on the cluster's own docker
// network, restricted to nodes carrying the kwok.x-k8s.io/node=fake annotation so real nodes are
// never touched. Idempotent: no-op if already running.
// https://kwok.sigs.k8s.io/docs/user/kwok-out-cluster/
//
// The container needs a kubeconfig pointed at the API server's address as seen from its own
// docker network (e.g. https://<cluster>-control-plane:6443), not the host-facing address (e.g.
// https://127.0.0.1:<port>) that kubeconfigPath contains - internalAPIServerAddress supplies
// that network-internal address explicitly (for a kind-provisioned target, orchestrate.Setup
// auto-derives it from cluster.name; a hand-supplied non-kind cluster must set it directly).
func RunController(ctx context.Context, kubeconfigPath, internalAPIServerAddress, targetName string) error {
	name := controllerName(targetName)
	internalKubeconfigPath := controllerKubeconfigPath(targetName)

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
	internalKubeconfig, err := buildInternalKubeconfig(kubeconfigPath, internalAPIServerAddress)
	if err != nil {
		return fmt.Errorf("building internal kubeconfig: %w", err)
	}
	if err := os.WriteFile(internalKubeconfigPath, internalKubeconfig, 0o600); err != nil {
		return fmt.Errorf("writing internal kubeconfig: %w", err)
	}

	cmd := exec.CommandContext(ctx, "docker",
		"run", "--rm", "-d",
		"--name", name,
		"--network", "kind",
		"-v", internalKubeconfigPath+":/kubeconfig:ro",
		ControllerImage,
		"--kubeconfig=/kubeconfig",
		"--manage-all-nodes=false",
		"--manage-nodes-with-annotation-selector="+NodeAnnotation+"="+NodeAnnotationOK,
		// Without this, kwok-controller ignores applied Stage CRD objects entirely and falls
		// back to its built-in pod stages - our custom pod-complete-armada stage has no
		// built-in equivalent, so without --enable-crds=Stage it never fires.
		"--enable-crds=Stage",
	)
	runOut, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("starting kwok-controller: %w: %s", err, runOut)
	}
	return nil
}

// buildInternalKubeconfig loads the kubeconfig at kubeconfigPath and returns a copy with the
// current context's cluster server URL replaced by internalAPIServerAddress - the kwok-controller
// container needs the API server's docker-network-internal address, not the host-facing one the
// on-disk kubeconfig points at.
func buildInternalKubeconfig(kubeconfigPath, internalAPIServerAddress string) ([]byte, error) {
	rawConfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("loading %s: %w", kubeconfigPath, err)
	}
	context, ok := rawConfig.Contexts[rawConfig.CurrentContext]
	if !ok {
		return nil, fmt.Errorf("%s: current-context %q not found", kubeconfigPath, rawConfig.CurrentContext)
	}
	cluster, ok := rawConfig.Clusters[context.Cluster]
	if !ok {
		return nil, fmt.Errorf("%s: cluster %q not found", kubeconfigPath, context.Cluster)
	}
	cluster.Server = internalAPIServerAddress
	return clientcmd.Write(*rawConfig)
}

// TeardownController stops the standalone kwok-controller container and removes its bind-mounted
// kubeconfig. Fake nodes are real objects in the cluster's own etcd (not disposable with the
// container) - see DeleteFakeNodes.
func TeardownController(ctx context.Context, targetName string) error {
	out, err := exec.CommandContext(ctx, "docker", "rm", "-f", controllerName(targetName)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("stopping kwok-controller: %w: %s", err, out)
	}
	if err := os.Remove(controllerKubeconfigPath(targetName)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing internal kubeconfig: %w", err)
	}
	return nil
}
