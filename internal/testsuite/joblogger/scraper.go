package joblogger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	pkgerrors "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

func (srv *JobLogger) runScraper(ctx context.Context) error {
	ticker := time.NewTicker(srv.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			wg := sync.WaitGroup{}

			srv.podMap.Range(srv.iterate(ctx, &wg))

			wg.Wait()
		}
	}
}

func (srv *JobLogger) iterate(ctx context.Context, wg *sync.WaitGroup) func(k, v interface{}) bool {
	return func(k, v interface{}) bool {
		wg.Add(1)
		defer wg.Done()
		info, ok := v.(*podInfo)
		if !ok {
			_, _ = fmt.Fprintf(srv.out, "invalid value for key: %s\n", k)
			return true
		}

		if info.hasStarted() && !info.Scraped {
			go func() {
				err := srv.scrape(ctx, wg, info)
				if err != nil {
					_, _ = fmt.Fprintf(srv.out, "%v\n", err)
				}
			}()
		}

		return true
	}
}

func (srv *JobLogger) scrape(ctx context.Context, wg *sync.WaitGroup, info *podInfo) error {
	wg.Add(1)
	defer wg.Done()

	clientset := srv.clientsets[info.Kubectx]
	logs, err := srv.getLogs(ctx, clientset, info.Name)
	if err != nil {
		return pkgerrors.WithMessagef(err, "error fetching logs for pod %s in kubectx %s: %v", info.Name, info.Kubectx, err)
	}

	info.appendLog(logs)

	if info.hasCompleted() {
		info.Scraped = true
	}

	return nil
}

func (srv *JobLogger) getLogs(ctx context.Context, clientset *kubernetes.Clientset, pod string) (*bytes.Buffer, error) {
	podLogOpts := v1.PodLogOptions{}
	stream, err := clientset.CoreV1().Pods(srv.namespace).GetLogs(pod, &podLogOpts).Stream(ctx)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "error getting log stream from Kubernetes API")
	}

	logs, err := read(stream)
	if err != nil {
		return nil, pkgerrors.WithMessage(err, "error reading log stream")
	}

	return logs, nil
}

func read(stream io.ReadCloser) (*bytes.Buffer, error) {
	defer stream.Close()
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, stream)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
