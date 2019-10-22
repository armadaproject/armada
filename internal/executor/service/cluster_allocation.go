package service

import (
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
)

const PodNamePrefix string = "batch-"

type ClusterAllocationService struct {
	leaseService       LeaseService
	utilisationService UtilisationService
	clusterContext     context.ClusterContext
}

func NewClusterAllocationService(
	clusterContext context.ClusterContext,
	leaseService LeaseService,
	utilisationService UtilisationService) *ClusterAllocationService {

	return &ClusterAllocationService{
		leaseService:       leaseService,
		utilisationService: utilisationService,
		clusterContext:     clusterContext}
}

func (allocationService *ClusterAllocationService) AllocateSpareClusterCapacity() {
	availableResource, err := allocationService.utilisationService.GetAvailableClusterCapacity()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	newJobs, err := allocationService.leaseService.RequestJobLeases(availableResource)

	cpu := (*availableResource)["cpu"]
	memory := (*availableResource)["memory"]
	log.Infof("Requesting new jobs with free resource cpu: %d, memory %d. Received %d new jobs. ", cpu.AsDec(), memory.Value(), len(newJobs))

	if err != nil {
		log.Errorf("Failed to lease new jobs because %s", err)
		return
	} else {
		for _, job := range newJobs {
			_, err = allocationService.submitJob(job)
			if err != nil {
				log.Errorf("Failed to submit job %s because %s", job.Id, err)
			}
		}
	}
}

func (allocationService *ClusterAllocationService) submitJob(job *api.Job) (*v1.Pod, error) {
	pod := createPod(job)
	return allocationService.clusterContext.SubmitPod(pod)
}

func createPod(job *api.Job) *v1.Pod {
	labels := createLabels(job)
	setRestartPolicyNever(job.PodSpec)

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   PodNamePrefix + job.Id,
			Labels: labels,
		},
		Spec: *job.PodSpec,
	}

	return &pod
}

func setRestartPolicyNever(podSpec *v1.PodSpec) {
	podSpec.RestartPolicy = v1.RestartPolicyNever
}

func createLabels(job *api.Job) map[string]string {
	labels := make(map[string]string)

	labels[domain.JobId] = job.Id
	labels[domain.JobSetId] = job.JobSetId
	labels[domain.Queue] = job.Queue

	return labels
}
