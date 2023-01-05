package scheduler

import (
	"k8s.io/client-go/kubernetes/fake"
	fakecoordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1/fake"
	"testing"
	"time"
)

func TestK8sLeaderContyroller(t *testing.T) {
	client := fake.NewSimpleClientset()
	coordinationClient := client.CoordinationV1().(*fakecoordinationv1.FakeCoordinationV1)

	config := LeaderConfig{
		LeaseLockName:      "",
		LeaseLockNamespace: "",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
		PodName:            "",
	}

	controller := NewKubernetesLeaderController(config, coordinationClient)

}
