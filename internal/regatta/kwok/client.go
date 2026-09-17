package kwok

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClientset builds a typed Kubernetes clientset from a kubeconfig path (or the default
// loading rules - KUBECONFIG env var, then $HOME/.kube/config - when kubeconfigPath is empty).
//
// kindClusterName, if non-empty, pins the client to the "kind-<kindClusterName>" context rather
// than trusting the kubeconfig file's current-context. This matters because `kind create
// cluster` rewrites current-context as a side effect - with two kind clusters in the same
// kubeconfig file, a second `kind create cluster` (e.g. for a second regatta cluster target)
// silently flips which cluster an already-running or about-to-run target connects to if it
// relies on current-context instead of a pinned context name.
func NewClientset(kubeconfigPath, kindClusterName string) (kubernetes.Interface, error) {
	restConfig, err := buildRestConfig(kubeconfigPath, kindClusterName)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("building kube clientset: %w", err)
	}
	return clientset, nil
}

func buildRestConfig(kubeconfigPath, kindClusterName string) (*rest.Config, error) {
	config, err := ResolveKubeconfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}
	overrides := &clientcmd.ConfigOverrides{}
	if kindClusterName != "" {
		overrides.CurrentContext = "kind-" + kindClusterName
	}
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: config},
		overrides,
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("building kube client config: %w", err)
	}
	return restConfig, nil
}

// ResolveKubeconfig returns kubeconfigPath if set, otherwise the default kubeconfig path
// resolved via the standard KUBECONFIG/$HOME/.kube/config loading rules.
func ResolveKubeconfig(kubeconfigPath string) (string, error) {
	if kubeconfigPath != "" {
		return kubeconfigPath, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	resolved := rules.GetDefaultFilename()
	if resolved == "" {
		return "", fmt.Errorf("no kubeconfig found via KUBECONFIG or default loading rules")
	}
	return resolved, nil
}
