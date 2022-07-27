package joblogger

import (
	"context"
	"fmt"
	"time"

	pkgerrors "github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func (srv *JobLogger) runWatcher(ctx context.Context, kubectx string, clientset *kubernetes.Clientset) error {
	ticker := time.NewTicker(srv.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C: // Print a summary of what happened in this interval.
			pods, err := srv.listPods(ctx, clientset)
			if err != nil {
				return err
			}

			if err := srv.upsertPodMap(kubectx, pods); err != nil {
				return err
			}
		}
	}
}

func (srv *JobLogger) listPods(ctx context.Context, clientset *kubernetes.Clientset) ([]*v1.Pod, error) {
	var labelSelector string
	if srv.queue != "" {
		labelSelector = fmt.Sprintf("armada_queue_id=%s", srv.queue)
	}
	listOpts := metav1.ListOptions{LabelSelector: labelSelector}
	pods, err := clientset.CoreV1().Pods(srv.namespace).List(ctx, listOpts)
	if err != nil {
		return nil, err
	}
	return srv.applyAdditionalPodFilters(pods.Items), nil
}

func (srv *JobLogger) applyAdditionalPodFilters(pods []v1.Pod) []*v1.Pod {
	var filtered []*v1.Pod

	for i := range pods {
		pod := &pods[i]
		if pod.Annotations["armada_jobset_id"] == srv.jobSetId {
			filtered = append(filtered, pod)
		}
	}
	return filtered
}

func (srv *JobLogger) upsertPodMap(kubectx string, pods []*v1.Pod) error {
	for i := range pods {
		pod := pods[i]
		actual, loaded := srv.podMap.LoadOrStore(pod.Name, newPodInfo(kubectx, pod))
		if loaded {
			p, ok := actual.(*podInfo)
			if !ok {
				return pkgerrors.Errorf("invalid entry stored for key %s", pod.Name)
			}
			p.Phase = pod.Status.Phase
		}
	}
	return nil
}
