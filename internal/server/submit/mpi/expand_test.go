package mpi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

var enabledConfig = configuration.SubmissionConfig{
	AllowGangServices: true,
	MpiJobs:           configuration.MpiJobsConfig{Enabled: true, BootstrapImage: "armada-mpi:test"},
}

func testIdGenerator() string {
	return "id"
}

func mpiItem() *api.JobSubmitRequestItem {
	return &api.JobSubmitRequestItem{
		Namespace:   "namespace",
		ClientId:    "client",
		Labels:      map[string]string{"team": "cfd"},
		Annotations: map[string]string{"note": "kept"},
		Services:    []*api.ServiceConfig{{Type: api.ServiceType_Headless, Ports: []uint32{8080}}},
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{{
				Name:    "main",
				Image:   "openmpi:test",
				Command: []string{"mpirun"},
				Args:    []string{"-np", "4", "/hello"},
				Env:     []v1.EnvVar{{Name: "OMPI_MCA_plm_rsh_agent", Value: "set-by-user"}},
			}},
		},
		Mpi: &api.MpiJobConfig{Workers: 2, SlotsPerWorker: 2},
	}
}

func envVar(container v1.Container, name string) string {
	for _, env := range container.Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

func TestExpandMpiJobs(t *testing.T) {
	other := &api.JobSubmitRequestItem{Namespace: "namespace", PodSpec: &v1.PodSpec{}}
	request := &api.JobSubmitRequest{JobRequestItems: []*api.JobSubmitRequestItem{other, mpiItem()}}

	require.NoError(t, ExpandMpiJobs(request, enabledConfig, testIdGenerator))

	require.Len(t, request.JobRequestItems, 4, "expected the other job, the launcher and two workers")
	assert.Same(t, other, request.JobRequestItems[0], "jobs that are not MPI jobs are left as they are")
	launcher, workers := request.JobRequestItems[1], request.JobRequestItems[2:]

	for _, job := range request.JobRequestItems[1:] {
		assert.Nil(t, job.Mpi)
		assert.Equal(t, "namespace", job.Namespace)
		assert.Equal(t, "kept", job.Annotations["note"])
		assert.Equal(t, "cfd", job.Labels["team"])
		assert.Equal(t, "mpi-id", job.Annotations[constants.GangIdAnnotation])
		assert.Equal(t, "3", job.Annotations[constants.GangCardinalityAnnotation])
		assert.Equal(t, "true", job.Annotations[constants.GangServiceAnnotation])
		assert.Equal(t, constants.GangFailurePolicyCancelGang, job.Annotations[constants.GangFailurePolicyAnnotation])

		podSpec := job.GetMainPodSpec()
		require.Len(t, podSpec.InitContainers, 1)
		assert.Equal(t, "armada-mpi:test", podSpec.InitContainers[0].Image)
		assert.Equal(t, v1.PullIfNotPresent, podSpec.InitContainers[0].ImagePullPolicy)
		assert.Equal(t, []string{"/app/armada-mpi", "install", "/opt/armada-mpi"}, podSpec.InitContainers[0].Command)
		assert.Equal(t, podSpec.InitContainers[0].Resources.Requests, podSpec.InitContainers[0].Resources.Limits)
		require.Len(t, podSpec.Volumes, 1)
		assert.NotNil(t, podSpec.Volumes[0].EmptyDir)
		assert.Equal(t, "/opt/armada-mpi", podSpec.Containers[0].VolumeMounts[0].MountPath)
		assert.Equal(t, "openmpi:test", podSpec.Containers[0].Image)
	}

	assert.Equal(t, "true", launcher.Annotations[constants.GangLeaderAnnotation])
	assert.Equal(t, constants.MpiRoleLauncher, launcher.Labels[constants.MpiRoleLabel])
	assert.Equal(t, "client", launcher.ClientId)
	assert.Len(t, launcher.Services, 1)
	launcherPod := launcher.GetMainPodSpec()
	assert.Equal(t, "launcher", launcherPod.Hostname)
	launcherContainer := launcherPod.Containers[0]
	assert.Equal(t, []string{"/opt/armada-mpi/armada-mpi", "launch", "--", "mpirun"}, launcherContainer.Command)
	assert.Equal(t, []string{"-np", "4", "/hello"}, launcherContainer.Args)
	assert.Equal(t, "2", envVar(launcherContainer, "ARMADA_MPI_WORKERS"))
	assert.Equal(t, "2", envVar(launcherContainer, "ARMADA_MPI_SLOTS_PER_WORKER"))
	assert.Equal(t, "/opt/armada-mpi/armada-mpi-agent", envVar(launcherContainer, "PRTE_MCA_plm_ssh_agent"))
	assert.Equal(t, "/opt/armada-mpi/hostfile.hydra", envVar(launcherContainer, "HYDRA_HOST_FILE"))
	assert.Equal(t, "set-by-user", envVar(launcherContainer, "OMPI_MCA_plm_rsh_agent"), "the environment of the user wins")

	for i, worker := range workers {
		assert.NotContains(t, worker.Annotations, constants.GangLeaderAnnotation)
		assert.Equal(t, constants.MpiRoleWorker, worker.Labels[constants.MpiRoleLabel])
		assert.Empty(t, worker.Services)
		workerPod := worker.GetMainPodSpec()
		assert.Equal(t, []string{"/opt/armada-mpi/armada-mpi", "worker"}, workerPod.Containers[0].Command)
		assert.Empty(t, workerPod.Containers[0].Args)
		assert.Equal(t, []string{"worker-0", "worker-1"}[i], workerPod.Hostname)
		assert.Equal(t, []string{"client-worker-0", "client-worker-1"}[i], worker.ClientId)
	}
}

func TestExpandMpiJobs_WorkerPodSpec(t *testing.T) {
	item := mpiItem()
	item.Mpi.SlotsPerWorker = 0
	item.Mpi.WorkerPodSpec = &v1.PodSpec{Containers: []v1.Container{{Name: "worker", Image: "worker:test", Command: []string{"ignored"}}}}
	request := &api.JobSubmitRequest{JobRequestItems: []*api.JobSubmitRequestItem{item}}

	require.NoError(t, ExpandMpiJobs(request, enabledConfig, testIdGenerator))

	require.Len(t, request.JobRequestItems, 3)
	launcherContainer := request.JobRequestItems[0].GetMainPodSpec().Containers[0]
	assert.Equal(t, "openmpi:test", launcherContainer.Image)
	assert.Equal(t, "1", envVar(launcherContainer, "ARMADA_MPI_SLOTS_PER_WORKER"), "slots per worker defaults to 1")
	workerContainer := request.JobRequestItems[1].GetMainPodSpec().Containers[0]
	assert.Equal(t, "worker:test", workerContainer.Image)
	assert.Equal(t, []string{"/opt/armada-mpi/armada-mpi", "worker"}, workerContainer.Command)
	// The pod spec given by the user is a template: it must not be modified.
	assert.Equal(t, []string{"ignored"}, item.Mpi.WorkerPodSpec.Containers[0].Command)
	assert.Empty(t, item.Mpi.WorkerPodSpec.InitContainers)
}

func TestExpandMpiJobs_Invalid(t *testing.T) {
	tests := map[string]struct {
		config configuration.SubmissionConfig
		modify func(item *api.JobSubmitRequestItem)
	}{
		"MPI jobs not enabled": {
			config: configuration.SubmissionConfig{AllowGangServices: true, MpiJobs: configuration.MpiJobsConfig{BootstrapImage: "image"}},
		},
		"gang services not enabled": {
			config: configuration.SubmissionConfig{MpiJobs: configuration.MpiJobsConfig{Enabled: true, BootstrapImage: "image"}},
		},
		"no bootstrap image": {
			config: configuration.SubmissionConfig{AllowGangServices: true, MpiJobs: configuration.MpiJobsConfig{Enabled: true}},
		},
		"no workers": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) { item.Mpi.Workers = 0 },
		},
		"gang annotation set": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) { item.Annotations[constants.GangIdAnnotation] = "mine" },
		},
		"no pod spec": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) { item.PodSpec = nil },
		},
		"no launcher command": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) { item.PodSpec.Containers[0].Command = nil },
		},
		"hostname set": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) { item.PodSpec.Hostname = "mine" },
		},
		"worker pod spec without containers": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) { item.Mpi.WorkerPodSpec = &v1.PodSpec{} },
		},
		"volume name taken": {
			config: enabledConfig,
			modify: func(item *api.JobSubmitRequestItem) {
				item.PodSpec.Volumes = []v1.Volume{{Name: "armada-mpi"}}
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			item := mpiItem()
			if tc.modify != nil {
				tc.modify(item)
			}
			request := &api.JobSubmitRequest{JobRequestItems: []*api.JobSubmitRequestItem{item}}
			assert.Error(t, ExpandMpiJobs(request, tc.config, testIdGenerator))
		})
	}
}
