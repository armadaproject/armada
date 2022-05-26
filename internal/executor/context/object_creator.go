package context

import (
	"context"
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/cluster"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Service for creating k8s objects while limiting
// the number of objects created per second and the number of concurrent calls to k8s.
type KubernetesObjectCreator struct {
	kubernetesClient kubernetes.Interface
	// TODO: I don't think we need this one
	kubernetesClientProvider cluster.KubernetesClientProvider
	// Queue of objects to be created.
	ch chan *QueuedKubernetesObject
	// Limit the number of objects created per second.
	maxObjectsPerSecond uint
	// Each object is created in a separate goroutine.
	// At most maxThreads such goroutines are active concurrently.
	maxThreads uint
}

// QueuedKubernetesObject wraps a k8s object to be created.
// We use it to bundle context with an object to create.
type QueuedKubernetesObject struct {
	ctx      context.Context
	obj      interface{}
	user     string
	groups   []string
	callback func(interface{}, error)
}

func (srv *KubernetesObjectCreator) SubmitPod(ctx context.Context, obj *v1.Pod, user string, groups []string, callback func(interface{}, error)) error {
	return srv.submitK8sObject(ctx, obj, user, groups)
}

func (srv *KubernetesObjectCreator) SubmitService(ctx context.Context, obj *v1.Service, user string, groups []string, callback func(interface{}, error)) error {
	return srv.submitK8sObject(ctx, obj, user, groups)
}

func (srv *KubernetesObjectCreator) SubmitIngress(ctx context.Context, obj *networking.Ingress, user string, groups []string, callback func(interface{}, error)) error {
	return srv.submitK8sObject(ctx, obj, user, groups)
}

func (srv *KubernetesObjectCreator) SubmitPodAsync(ctx context.Context, obj *v1.Pod, user string, groups []string, callback func(interface{}, error)) {
	srv.submitK8sObjectAsync(ctx, obj, user, groups, callback)
}

func (srv *KubernetesObjectCreator) SubmitServiceAsync(ctx context.Context, obj *v1.Service, user string, groups []string, callback func(interface{}, error)) {
	srv.submitK8sObjectAsync(ctx, obj, user, groups, callback)
}

func (srv *KubernetesObjectCreator) SubmitIngressAsync(ctx context.Context, obj *networking.Ingress, user string, groups []string, callback func(interface{}, error)) {
	srv.submitK8sObjectAsync(ctx, obj, user, groups, callback)
}

// submitK8sObject queues up a k8s object for submission to the k8s client
// and waits for it to be processed.
func (srv *KubernetesObjectCreator) submitK8sObject(ctx context.Context, obj interface{}, user string, groups []string) error {
	ch := make(chan error)
	callback := func(_ interface{}, err error) {
		ch <- err
	}
	srv.submitK8sObjectAsync(ctx, obj, user, groups, callback)
	return <-ch
}

// submitK8sObjectAsync queues up a k8s object to be submitted to the k8s client.
// Callback is called with the created object and any error when the object is created or if ctx is closed,
// whichever occurs first.
func (srv *KubernetesObjectCreator) submitK8sObjectAsync(ctx context.Context, obj interface{}, user string, groups []string, callback func(interface{}, error)) {

	// Replace nils with defaults to reduce risk of nil references.
	if callback == nil {
		callback = func(interface{}, error) {}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Can't submit a nil pod.
	if obj == nil {
		err := &armadaerrors.ErrInvalidArgument{
			Name:  "obj",
			Value: obj,
		}
		callback(nil, err)
		return
	}

	// Special object with context to put on the queue.
	v := &QueuedKubernetesObject{
		ctx:      ctx,
		obj:      obj,
		user:     user,
		groups:   groups,
		callback: callback,
	}

	// Because sending on a channel may block, we send in a separate goroutine.
	// If ctx is closed before sending succeeds, we exit by calling the callback with the context error.
	go func() {
		select {
		case <-ctx.Done():
			callback(nil, ctx.Err())
		case srv.ch <- v:
			return
		}
	}()
}

// Run starts a service that continously creates queued k8s object.
// Objects are created concurrently in separate goroutines.
//
// At most srv.maxObjectsPerSecond such goroutines are created per second.
// At most srv.maxThreads such goroutines may run concurrently at any time.
func (srv *KubernetesObjectCreator) Run(ctx context.Context) error {

	// Use a channel with capacity srv.maxThreads to limit the max number of concurrent goroutines.
	concurrencyLimitChan := make(chan int, srv.maxThreads)

	// This ticker controls the max rate at which goroutines are created.
	intervalSeconds := 1 / float64(srv.maxObjectsPerSecond)
	intervalNanos := int(intervalSeconds * 1e9)
	ticker := time.NewTicker(time.Duration(intervalNanos))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Get an object from the queue and create it asyncronously.
			// This can happen at most every interval.
			// Blocking on receiving on the channel will cause the ticker to pause.
			queueObj := <-srv.ch
			if queueObj == nil {
				break
			}

			// To count the number of concurrent objects, hijack the callback function.
			// Takes a value out of the channel to allow the creation of a new goroutine.
			queueObj.callback = func(obj interface{}, err error) {
				<-concurrencyLimitChan
				queueObj.callback(obj, err)
			}

			// Wait until we can send on the channel or until ctx is cancelled.
			// The value sent is arbitrary.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case concurrencyLimitChan <- 0:
				go srv.createObject(ctx, queueObj)
			}
		}
	}
}

func (srv *KubernetesObjectCreator) createObject(ctx context.Context, queueObj *QueuedKubernetesObject) {

	// On exit, call the callback with the created object and any error.
	var createdObj interface{}
	var err error
	defer func() {
		if queueObj.callback != nil {
			queueObj.callback(createdObj, err)
		}
	}()

	// If the context bundled with the object is closed, exit.
	err = queueObj.ctx.Err()
	if err != nil {
		return
	}

	// If the object is nil, return an error.
	if queueObj.obj == nil {
		err = &armadaerrors.ErrInvalidArgument{
			Name:  "pod",
			Value: queueObj.obj,
		}
		return
	}

	// Create the object.
	switch o := queueObj.obj.(type) {
	case *v1.Pod:
		var client kubernetes.Interface
		client, err = srv.kubernetesClientProvider.ClientForUser(queueObj.user, queueObj.groups)
		if err != nil {
			return
		}

		createdObj, err = client.CoreV1().Pods(o.Namespace).Create(queueObj.ctx, o, metav1.CreateOptions{})
		if err != nil {
			return
		}
	case *networking.Ingress:
		createdObj, err = srv.kubernetesClient.NetworkingV1().Ingresses(o.Namespace).Create(queueObj.ctx, o, metav1.CreateOptions{})
		if err != nil {
			return
		}
	case *v1.Service:
		createdObj, err = srv.kubernetesClient.CoreV1().Services(o.Namespace).Create(queueObj.ctx, o, metav1.CreateOptions{})
		if err != nil {
			return
		}
	default:
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "obj",
			Value:   o,
			Message: fmt.Sprintf("unsupported object type %T", queueObj.obj),
		}
	}
}
