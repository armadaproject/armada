package kwok

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClientset builds a typed Kubernetes clientset from a kubeconfig path (or the default
// loading rules - KUBECONFIG env var, then $HOME/.kube/config - when kubeconfigPath is empty).
func NewClientset(kubeconfigPath string) (kubernetes.Interface, error) {
	config, err := ResolveKubeconfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}
	restConfig, err := clientcmd.BuildConfigFromFlags("", config)
	if err != nil {
		return nil, fmt.Errorf("building kube client config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("building kube clientset: %w", err)
	}
	return clientset, nil
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
