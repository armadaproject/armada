package context

import (
	"context"
	"fmt"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/cluster"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
)

type KubernetesObjectCreator struct {
	// clusterId                string
	// pool                     string
	// deleteThreadCount        int
	// submittedPods            util.PodCache
	// podsToDelete             util.PodCache
	// podInformer              informer.PodInformer
	// nodeInformer             informer.NodeInformer
	// serviceInformer          informer.ServiceInformer
	// ingressInformer          network_informer.IngressInformer
	// stopper                  chan struct{}
	// kubernetesClient         kubernetes.Interface
	// eventInformer            informer.EventInformer
	//
	// TODO: I don't think we need this one
	kubernetesClientProvider cluster.KubernetesClientProvider
	ch                       chan kubernetesObjectWithCallback
}

//
type kubernetesObjectWithCallback struct {
	ctx      context.Context
	obj      interface{}
	user     string
	groups   []string
	callback func(interface{}, error)
}

func (srv *KubernetesObjectCreator) SubmitPodAsync(ctx context.Context, pod *v1.Pod, user string, groups []string, callback func(*v1.Pod, error)) error {
	return nil
}

func (srv *KubernetesObjectCreator) createObject(ctx context.Context, obj interface{}) error {
	switch o := obj.(type) {
	case *v1.Pod:
	case *networking.Ingress:
	case *v1.Service:
	default:
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "obj",
			Value:   o,
			Message: fmt.Sprintf("unsupported object type %T", obj),
		})
	}
	return nil
}

// func (srv *KubernetesObjectCreator) createPod(pod *v1.Pod) error {
// 	client, err := srv.kubernetesClientProvider.ClientForUser(owner, ownerGroups)
// 	if err != nil {
// 		return nil, err
// 	}

// 	returnedPod, err := client.CoreV1().Pods(pod.Namespace).Create(ctx.Background(), pod, metav1.CreateOptions{})
// 	if err != nil {
// 		return nil, err
// 	}
// }

// func (c *KubernetesClusterContext) SubmitPod(pod *v1.Pod, owner string, ownerGroups []string) (*v1.Pod, error) {

// 	c.submittedPods.Add(pod)
// 	ownerClient, err := c.kubernetesClientProvider.ClientForUser(owner, ownerGroups)
// 	if err != nil {
// 		return nil, err
// 	}

// 	returnedPod, err := ownerClient.CoreV1().Pods(pod.Namespace).Create(ctx.Background(), pod, metav1.CreateOptions{})

// 	if err != nil {
// 		c.submittedPods.Delete(util.ExtractPodKey(pod))
// 	}
// 	return returnedPod, err
// }

// func (c *KubernetesClusterContext) SubmitService(service *v1.Service) (*v1.Service, error) {
// 	return c.kubernetesClient.CoreV1().Services(service.Namespace).Create(ctx.Background(), service, metav1.CreateOptions{})
// }

// func (c *KubernetesClusterContext) SubmitIngress(ingress *networking.Ingress) (*networking.Ingress, error) {
// 	return c.kubernetesClient.NetworkingV1().Ingresses(ingress.Namespace).Create(ctx.Background(), ingress, metav1.CreateOptions{})
// }
