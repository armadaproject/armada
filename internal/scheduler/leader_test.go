package scheduler

import (
	"context"
	"k8s.io/client-go/kubernetes/fake"
	fakecoordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1/fake"
	"testing"
	"time"
)

func TestK8sLeaderContyroller(t *testing.T) {
	client := fake.NewSimpleClientset()
	coordinationClient := client.CoordinationV1().(*fakecoordinationv1.FakeCoordinationV1)

	config := LeaderConfig{
		LeaseLockName:      "armada-test",
		LeaseLockNamespace: "armada-test",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "armada-test-123",
	}

	controller := NewKubernetesLeaderController(config, coordinationClient)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	controller.Run(ctx)

}
