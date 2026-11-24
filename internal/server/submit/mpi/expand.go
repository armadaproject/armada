// Package mpi expands the MPI jobs of a submit request into the gangs of ordinary jobs that run them.
package mpi

import (
	"fmt"
	"path"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/mpibootstrap"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	// bootstrapVolumeName names the volume through which the armada-mpi program reaches the containers of the user.
	bootstrapVolumeName = "armada-mpi"
	// bootstrapInitContainerName names the init container that copies the armada-mpi program onto that volume.
	bootstrapInitContainerName = "armada-mpi-install"
	// bootstrapImageBinary is the path of the armada-mpi program in the bootstrap image.
	bootstrapImageBinary = "/app/armada-mpi"
)

// ExpandMpiJobs replaces each item of the request that describes an MPI job, see api.JobSubmitRequestItem.Mpi, by
// the ordinary jobs that run it: a launcher followed by its workers. Items that are not MPI jobs are left as they are.
//
// The jobs of an MPI job form a gang. They share a gang service, so that the launcher and the workers reach each
// other by name; the launcher is the gang leader, so that the workers are stopped once it is done; and a failure of
// any of them cancels the rest. Each pod gets the armada-mpi program, through which the launcher starts MPI
// processes on the workers without ssh.
//
// It must run before the request is validated, so that the expanded jobs are validated like any other.
func ExpandMpiJobs(request *api.JobSubmitRequest, config configuration.SubmissionConfig, idGenerator func() string) error {
	expanded := make([]*api.JobSubmitRequestItem, 0, len(request.JobRequestItems))
	for _, item := range request.JobRequestItems {
		if item.GetMpi() == nil {
			expanded = append(expanded, item)
			continue
		}
		jobs, err := expand(item, config, "mpi-"+idGenerator())
		if err != nil {
			return err
		}
		expanded = append(expanded, jobs...)
	}
	request.JobRequestItems = expanded
	return nil
}

func expand(item *api.JobSubmitRequestItem, config configuration.SubmissionConfig, gangId string) ([]*api.JobSubmitRequestItem, error) {
	if !config.MpiJobs.Enabled || !config.AllowGangServices || config.MpiJobs.BootstrapImage == "" {
		return nil, errors.New("MPI jobs are not enabled")
	}
	mpiConfig := item.GetMpi()
	if mpiConfig.Workers < 1 {
		return nil, errors.New("an MPI job must have at least one worker")
	}
	for _, annotation := range []string{
		constants.GangIdAnnotation,
		constants.GangCardinalityAnnotation,
		constants.GangServiceAnnotation,
		constants.GangLeaderAnnotation,
		constants.GangFailurePolicyAnnotation,
	} {
		if _, ok := item.Annotations[annotation]; ok {
			return nil, errors.Errorf("an MPI job may not set the annotation %s: Armada sets it", annotation)
		}
	}
	launcherSpec := item.GetMainPodSpec()
	if err := validatePodSpec(launcherSpec, "launcher"); err != nil {
		return nil, err
	}
	if len(launcherSpec.Containers[0].Command) == 0 {
		return nil, errors.New(
			"the first container of the launcher of an MPI job must set its command, which starts the MPI job, e.g., with mpirun",
		)
	}
	workerSpec := mpiConfig.WorkerPodSpec
	if workerSpec == nil {
		workerSpec = launcherSpec
	} else if err := validatePodSpec(workerSpec, "workers"); err != nil {
		return nil, err
	}
	slotsPerWorker := mpiConfig.SlotsPerWorker
	if slotsPerWorker == 0 {
		slotsPerWorker = 1
	}

	jobs := make([]*api.JobSubmitRequestItem, 0, mpiConfig.Workers+1)
	cardinality := strconv.FormatUint(uint64(mpiConfig.Workers)+1, 10)

	launcher := member(item, gangId, cardinality, constants.MpiRoleLauncher, launcherSpec.DeepCopy())
	launcher.Annotations[constants.GangLeaderAnnotation] = "true"
	launcherPod := launcher.PodSpecs[0]
	launcherPod.Hostname = mpibootstrap.LauncherHostname
	addBootstrap(launcherPod, config.MpiJobs.BootstrapImage)
	launcherContainer := &launcherPod.Containers[0]
	launcherContainer.Command = append(
		[]string{path.Join(mpibootstrap.DefaultDir, mpibootstrap.BinaryName), "launch", "--"},
		launcherContainer.Command...,
	)
	addEnvVarsIfNotSet(launcherContainer, launcherEnvVars(mpiConfig.Workers, slotsPerWorker))
	jobs = append(jobs, launcher)

	for i := 0; i < int(mpiConfig.Workers); i++ {
		worker := member(item, gangId, cardinality, constants.MpiRoleWorker, workerSpec.DeepCopy())
		// Ingresses and services of the item belong to the launcher, which is the job the user described.
		worker.Ingress = nil
		worker.Services = nil
		if worker.ClientId != "" {
			worker.ClientId = fmt.Sprintf("%s-worker-%d", item.ClientId, i)
		}
		workerPod := worker.PodSpecs[0]
		workerPod.Hostname = mpibootstrap.WorkerHostname(i)
		addBootstrap(workerPod, config.MpiJobs.BootstrapImage)
		workerContainer := &workerPod.Containers[0]
		workerContainer.Command = []string{path.Join(mpibootstrap.DefaultDir, mpibootstrap.BinaryName), "worker"}
		workerContainer.Args = nil
		jobs = append(jobs, worker)
	}
	return jobs, nil
}

func validatePodSpec(podSpec *v1.PodSpec, role string) error {
	if podSpec == nil || len(podSpec.Containers) == 0 {
		return errors.Errorf("the pod spec of the %s of an MPI job must have at least one container", role)
	}
	if podSpec.Hostname != "" || podSpec.Subdomain != "" {
		return errors.Errorf("the pod spec of the %s of an MPI job may not set hostname or subdomain: Armada sets them", role)
	}
	for _, volume := range podSpec.Volumes {
		if volume.Name == bootstrapVolumeName {
			return errors.Errorf("the pod spec of the %s of an MPI job may not have a volume named %s", role, bootstrapVolumeName)
		}
	}
	return nil
}

// member returns a copy of the item as one job of the gang that runs the MPI job, with podSpec as its pod spec.
func member(item *api.JobSubmitRequestItem, gangId, cardinality, role string, podSpec *v1.PodSpec) *api.JobSubmitRequestItem {
	job := proto.Clone(item).(*api.JobSubmitRequestItem)
	job.Mpi = nil
	job.PodSpec = nil
	job.PodSpecs = []*v1.PodSpec{podSpec}
	if job.Annotations == nil {
		job.Annotations = map[string]string{}
	}
	job.Annotations[constants.GangIdAnnotation] = gangId
	job.Annotations[constants.GangCardinalityAnnotation] = cardinality
	job.Annotations[constants.GangServiceAnnotation] = "true"
	job.Annotations[constants.GangFailurePolicyAnnotation] = constants.GangFailurePolicyCancelGang
	if job.Labels == nil {
		job.Labels = map[string]string{}
	}
	job.Labels[constants.MpiRoleLabel] = role
	return job
}

// addBootstrap makes the armada-mpi program available to the first container of the pod: an init container copies
// it from the bootstrap image onto a volume that the container mounts.
func addBootstrap(podSpec *v1.PodSpec, bootstrapImage string) {
	mount := v1.VolumeMount{Name: bootstrapVolumeName, MountPath: mpibootstrap.DefaultDir}
	podSpec.Volumes = append(podSpec.Volumes, v1.Volume{
		Name:         bootstrapVolumeName,
		VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
	})
	resources := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("100m"),
		v1.ResourceMemory: resource.MustParse("64Mi"),
	}
	podSpec.InitContainers = append(podSpec.InitContainers, v1.Container{
		Name:  bootstrapInitContainerName,
		Image: bootstrapImage,
		// Every pod of every MPI job needs the image, so do not pull it again if the node already has it.
		ImagePullPolicy: v1.PullIfNotPresent,
		Command:         []string{bootstrapImageBinary, "install", mpibootstrap.DefaultDir},
		VolumeMounts:    []v1.VolumeMount{mount},
		Resources:       v1.ResourceRequirements{Requests: resources, Limits: resources.DeepCopy()},
	})
	podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, mount)
}

// launcherEnvVars returns the environment that describes the job to the armada-mpi program in the launcher, and
// that points the supported MPI implementations at its agent and at the hostfiles it writes.
func launcherEnvVars(workers, slotsPerWorker uint32) []v1.EnvVar {
	agent := path.Join(mpibootstrap.DefaultDir, mpibootstrap.AgentBinaryName)
	openMpiHostfile := path.Join(mpibootstrap.DefaultDir, mpibootstrap.OpenMpiHostfileName)
	hydraHostfile := path.Join(mpibootstrap.DefaultDir, mpibootstrap.HydraHostfileName)
	return []v1.EnvVar{
		{Name: mpibootstrap.WorkersEnvVar, Value: strconv.FormatUint(uint64(workers), 10)},
		{Name: mpibootstrap.SlotsPerWorkerEnvVar, Value: strconv.FormatUint(uint64(slotsPerWorker), 10)},
		// Open MPI up to version 4. The launcher starts every daemon itself: a daemon on a worker has no agent
		// through which it could start further daemons.
		{Name: "OMPI_MCA_plm_rsh_agent", Value: agent},
		{Name: "OMPI_MCA_plm_rsh_no_tree_spawn", Value: "1"},
		{Name: "OMPI_MCA_orte_default_hostfile", Value: openMpiHostfile},
		// Open MPI from version 5, whose runtime is PRRTE.
		{Name: "PRTE_MCA_plm_ssh_agent", Value: agent},
		{Name: "PRTE_MCA_plm_ssh_no_tree_spawn", Value: "1"},
		{Name: "PRTE_MCA_prte_default_hostfile", Value: openMpiHostfile},
		// MPICH, whose process manager is Hydra.
		{Name: "HYDRA_LAUNCHER", Value: "rsh"},
		{Name: "HYDRA_LAUNCHER_EXEC", Value: agent},
		{Name: "HYDRA_HOST_FILE", Value: hydraHostfile},
	}
}

func addEnvVarsIfNotSet(container *v1.Container, envVars []v1.EnvVar) {
	set := make(map[string]bool, len(container.Env))
	for _, envVar := range container.Env {
		set[envVar.Name] = true
	}
	for _, envVar := range envVars {
		if !set[envVar.Name] {
			container.Env = append(container.Env, envVar)
		}
	}
}
