package proxy

import (
	"context"
	"net/url"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// RemoteExecutor wraps remotecommand.Executor to allow fakes in tests.
type RemoteExecutor interface {
	StreamWithContext(ctx context.Context, options remotecommand.StreamOptions) error
}

// SPDYExecutorFactory creates a RemoteExecutor for a given URL.
// The production implementation wraps remotecommand.NewSPDYExecutor.
// Tests inject a fake.
type SPDYExecutorFactory func(config *rest.Config, method string, url *url.URL) (RemoteExecutor, error)

// DefaultSPDYExecutorFactory is the production SPDYExecutorFactory.
func DefaultSPDYExecutorFactory(config *rest.Config, method string, url *url.URL) (RemoteExecutor, error) {
	exec, err := remotecommand.NewSPDYExecutor(config, method, url)
	if err != nil {
		return nil, err
	}
	return exec, nil
}
