package kwok

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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

// RunController starts the standalone kwok-controller container, restricted to nodes carrying
// the kwok.x-k8s.io/node=fake annotation so real nodes are never touched. Idempotent: no-op if
// already running. https://kwok.sigs.k8s.io/docs/user/kwok-out-cluster/
//
// The container needs a kubeconfig pointed at the API server's address as seen from its own
// network - internalAPIServerAddress supplies that explicitly (orchestrate.Setup fills in a
// default when the scenario leaves it unset: kind's own internal-DNS convention for a
// kind-provisioned target, or Kubeconfig's own server address otherwise). kind additionally
// joins the container to the "kind" docker network, needed to reach a kind cluster's
// control-plane container by that internal address - a real cluster reached over a normal
// network needs no special network attachment.
func RunController(ctx context.Context, kubeconfigPath, internalAPIServerAddress, targetName string, kind bool) error {
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
	internalKubeconfig, err := buildInternalKubeconfig(ctx, kubeconfigPath, internalAPIServerAddress)
	if err != nil {
		return fmt.Errorf("building internal kubeconfig: %w", err)
	}
	if err := os.WriteFile(internalKubeconfigPath, internalKubeconfig, 0o600); err != nil {
		return fmt.Errorf("writing internal kubeconfig: %w", err)
	}

	args := []string{"run", "--rm", "-d", "--name", name}
	if kind {
		args = append(args, "--network", "kind")
	}
	args = append(args,
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
	cmd := exec.CommandContext(ctx, "docker", args...)
	runOut, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("starting kwok-controller: %w: %s", err, runOut)
	}
	return nil
}

// KubeconfigServerAddress reports the current-context cluster's server address from the
// kubeconfig at kubeconfigPath - the default internalAPIServerAddress for a non-kind target (see
// orchestrate.setupCluster), since a real cluster reached over a normal network has no separate
// network-internal address to derive.
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

// buildInternalKubeconfig loads the kubeconfig at kubeconfigPath and returns a copy with the
// current context's cluster server URL replaced by internalAPIServerAddress - the kwok-controller
// container needs the API server's docker-network-internal address, not the host-facing one the
// on-disk kubeconfig points at. TLSServerName (needed for e.g. a proxied cluster's SNI
// routing) is left untouched, since it's a property of the proxy address, not the real backend.
//
// If the current user's AuthInfo uses an exec credential plugin, it's resolved here on the host - where the plugin binary and
// any session state it needs actually exist - into a static client certificate/key, and the Exec
// config is dropped. The kwok-controller container has neither the plugin binary nor that session
// state, so it could never run the plugin itself; this trades that off against the resolved
// cert's own lifetime, same as any short-lived credential; the fake nodes will need refreshing by
// re-running the target's setup after the cert expires.
func buildInternalKubeconfig(ctx context.Context, kubeconfigPath, internalAPIServerAddress string) ([]byte, error) {
	rawConfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("loading %s: %w", kubeconfigPath, err)
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

// resolveExecCredential runs authInfo.Exec's credential plugin (the "kubectl credential plugin"
// contract: https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins)
// and replaces authInfo's Exec config with the static client-certificate/key it returns, so the
// resulting kubeconfig no longer needs the plugin binary present to authenticate.
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
	if cred.Status.ClientCertificateData == "" || cred.Status.ClientKeyData == "" {
		return fmt.Errorf("%s: ExecCredential response had no client certificate/key", execCfg.Command)
	}

	authInfo.ClientCertificateData = []byte(cred.Status.ClientCertificateData)
	authInfo.ClientKeyData = []byte(cred.Status.ClientKeyData)
	authInfo.Exec = nil
	return nil
}

// execCommandContext builds the credential plugin's command per the exec plugin contract: its
// Env entries are appended to (not replacing) the host's own environment.
func execCommandContext(ctx context.Context, execCfg *clientcmdapi.ExecConfig) *exec.Cmd {
	cmd := exec.CommandContext(ctx, execCfg.Command, execCfg.Args...)
	cmd.Env = os.Environ()
	for _, e := range execCfg.Env {
		cmd.Env = append(cmd.Env, e.Name+"="+e.Value)
	}
	return cmd
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
