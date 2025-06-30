package validation

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/bidstore"
)

// requestValidator is a function that validates the entire JobSubmitRequest
type requestValidator func(msg *api.JobSubmitRequest, config configuration.SubmissionConfig) error

// itemValidator is a function that validates an individual JobSubmitRequestItem
type itemValidator func(spec *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error

var (
	requestValidators = []requestValidator{
		validateHasQueue,
		validateGangs,
		validateHasJobSetId,
		validateJobSetIdLength,
	}
	itemValidators = []itemValidator{
		validateHasNamespace,
		validateHasPodSpec,
		validatePodSpecSize,
		validateAffinity,
		validateResources,
		validateInitContainerCpu,
		validatePriorityClasses,
		validateTerminationGracePeriod,
		validateIngresses,
		validatePorts,
		validateClientId,
		validateTolerations,
		validatePriceBand,
	}
)

// ValidateSubmitRequest ensures that the incoming api.JobSubmitRequest is well-formed. It achieves this
// by applying a series of validators that each check a single aspect of the request. Validators may
// choose to validate the whole obSubmitRequest or just a single JobSubmitRequestItem.
// This function will return the error from the first validator that fails, or nil if all  validators pass.
func ValidateSubmitRequest(req *api.JobSubmitRequest, config configuration.SubmissionConfig) error {
	for _, validationFunc := range requestValidators {
		err := validationFunc(req, config)
		if err != nil {
			return err
		}
	}

	for _, item := range req.JobRequestItems {
		for _, validationFunc := range itemValidators {
			err := validationFunc(item, config)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Ensures the serialised size of the podspec (in bytes) is less than the maximum allowed in config.
// This check exists because excessively large podspecs can cause  issues for ETCd in the executor clusters.
func validatePodSpecSize(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()

	if uint(spec.Size()) > config.MaxPodSpecSizeBytes {
		return errors.Errorf(
			"Pod spec has a size of %d bytes which is greater than the maximum allowed size of %d",
			spec.Size(),
			config.MaxPodSpecSizeBytes)
	}
	return nil
}

// Ensures that ingresses define at least one port and that the same port isn't shared between multiple ingresses.
func validateIngresses(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	existingPortSet := make(map[uint32]int)

	for index, portConfig := range j.Ingress {
		if len(portConfig.Ports) == 0 {
			return fmt.Errorf("ingress contains zero ports. Each ingress should have at least one port")
		}

		for _, port := range portConfig.Ports {
			if existingIndex, existing := existingPortSet[port]; existing {
				return fmt.Errorf(
					"port %d has two ingress configurations, specified in ingress configs with indexes %d, %d. Each port should at maximum have one ingress configuration",
					port,
					existingIndex,
					index,
				)
			} else {
				existingPortSet[port] = index
			}
		}
	}
	return nil
}

// Ensures that the request has non-empty job set id field.
func validateHasJobSetId(j *api.JobSubmitRequest, _ configuration.SubmissionConfig) error {
	if len(j.JobSetId) == 0 {
		return fmt.Errorf("job set id is a required field")
	}
	return nil
}

// Ensures that the request has job set id field isn't too long.
func validateJobSetIdLength(j *api.JobSubmitRequest, _ configuration.SubmissionConfig) error {
	const maxJobSetIdChars = 1024
	if len(j.GetJobSetId()) >= maxJobSetIdChars {
		return fmt.Errorf("job set id of length %d must be less than max character length %d", len(j.GetJobSetId()), maxJobSetIdChars)
	}
	return nil
}

// Ensures that the request has non-empty namespace field.
func validateHasNamespace(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	if len(j.Namespace) == 0 {
		return fmt.Errorf("namespace is a required field")
	}
	return nil
}

// Validates that the JobSubmitRequestItem has exactly one podspec defined.
func validateHasPodSpec(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	if j.PodSpec == nil && len(j.PodSpecs) == 0 {
		return errors.Errorf("Job must contain at least one PodSpec")
	}

	if len(j.PodSpecs) > 0 && j.PodSpec != nil {
		return errors.Errorf("PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)")
	}

	if len(j.PodSpecs) > 1 {
		return errors.Errorf("Jobs with multiple pods are not supported")
	}

	return nil
}

// Ensures that the request has non-empty queue field.
func validateHasQueue(r *api.JobSubmitRequest, _ configuration.SubmissionConfig) error {
	if len(r.Queue) == 0 {
		return fmt.Errorf("queue is a required field")
	}
	return nil
}

// Ensures that each container exposes a given port at most once.
func validatePorts(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	existingPortSet := make(map[int32]int)
	for index, container := range spec.Containers {
		for _, port := range container.Ports {
			if existingIndex, existing := existingPortSet[port.ContainerPort]; existing {
				return fmt.Errorf(
					"container port %d is exposed multiple times, specified in containers with indexes %d, %d. Should only be exposed once",
					port.ContainerPort, existingIndex, index)
			} else {
				existingPortSet[port.ContainerPort] = index
			}
		}
	}
	return nil
}

// Ensures that the request does not define  a  PreferredDuringSchedulingIgnoredDuringExecution Affinity (which is
// not supported by Armada) and that any other affinities defined are valid.
func validateAffinity(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	affinity := j.GetMainPodSpec().Affinity
	if affinity == nil {
		return nil // No affinity to check
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil // No affinity to check
	}

	// We don't support PreferredDuringSchedulingIgnoredDuringExecution
	if len(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
		return fmt.Errorf("preferredDuringSchedulingIgnoredDuringExecution node affinity is not supported by Armada")
	}

	// Check that RequiredDuringSchedulingIgnoredDuringExecution is actually a valid affinity rule
	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		_, err := nodeaffinity.NewNodeSelector(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
		if err != nil {
			return fmt.Errorf("invalid RequiredDuringSchedulingIgnoredDuringExecution node affinity: %v", err)
		}
	}
	return nil
}

// Ensures that if a request specifies a ClientId, that clientID is not too long
func validateClientId(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	const maxClientIdChars = 100
	if len(j.GetClientId()) > maxClientIdChars {
		return fmt.Errorf("client id of length %d is greater than max allowed length of  %d", len(j.ClientId), maxClientIdChars)
	}
	return nil
}

func validatePriceBand(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	priceBand, present := j.Annotations[configuration.JobPriceBand]
	if present {
		_, valid := bidstore.PriceBandFromShortName[strings.ToUpper(priceBand)]
		if !valid {
			return fmt.Errorf("price band %s is not supported", priceBand)
		}
	}

	return nil
}

// Ensures that if a request specifies a PriorityClass, that priority class is supported by Armada.
func validatePriorityClasses(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}

	priorityClassName := spec.PriorityClassName
	if priorityClassName == "" {
		return nil
	}

	if exists := config.AllowedPriorityClassNames[priorityClassName]; !exists {
		return fmt.Errorf("priority class %s is not supported", priorityClassName)
	}
	return nil
}

// Ensures that the JobSubmitRequestItem's limits and requests are equal.
// Also  checks that  any resources defined are above minimum values set in  config
func validateResources(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	maxOversubscriptionByResource := config.MaxOversubscriptionByResourceRequest
	if maxOversubscriptionByResource == nil {
		maxOversubscriptionByResource = map[string]float64{}
	}
	for _, container := range spec.Containers {

		if len(container.Resources.Requests) == 0 && len(container.Resources.Requests) == 0 {
			return fmt.Errorf("container %v has no resources specified", container.Name)
		}

		if len(container.Resources.Requests) != len(container.Resources.Limits) {
			return fmt.Errorf("container %v defines different resources for requests and limits", container.Name)
		}

		for resource, request := range container.Resources.Requests {
			limit, ok := container.Resources.Limits[resource]
			if !ok {
				return fmt.Errorf("container %v defines %s for requests but not limits", container.Name, resource)
			}
			if limit.MilliValue() < request.MilliValue() {
				return fmt.Errorf("container %v defines %s with limits smaller than requests", container.Name, resource)
			}
			maxOversubscription, ok := maxOversubscriptionByResource[resource.String()]
			if !ok {
				maxOversubscription = 1.0
			}
			if float64(limit.MilliValue()) > maxOversubscription*float64(request.MilliValue()) {
				return fmt.Errorf("container %v defines %s with limits great than %.2f*requests", container.Name, resource, maxOversubscription)
			}
		}

		for rc, containerRsc := range container.Resources.Requests {
			serverRsc, nonEmpty := config.MinJobResources[rc]
			if nonEmpty && containerRsc.Value() < serverRsc.Value() {
				return fmt.Errorf(
					"container %q %s requests (%s) below server minimum (%s)",
					container.Name,
					rc,
					&containerRsc,
					&serverRsc,
				)
			}
		}
	}
	return nil
}

// jobAdapter turns JobSubmitRequestItem into a MinimalJob
// This is needed for gang information to be extracted
type jobAdapter struct {
	*api.JobSubmitRequestItem
}

// PriorityClassName is needed to fulfil the MinimalJob interface
func (j jobAdapter) PriorityClassName() string {
	podSpec := j.GetMainPodSpec()
	if podSpec != nil {
		return j.GetMainPodSpec().PriorityClassName
	}
	return ""
}

// Annotations is needed to fulfil the MinimalJob interface
func (j jobAdapter) Annotations() map[string]string {
	return j.GetAnnotations()
}

// Ensures that any gang jobs defined in the request are consistent.  This checks that all jobs in the same gang have
// the same:
//   - Cardinality
//   - MinimumCardinality
//   - Priority Class
//   - Node Uniformity
func validateGangs(request *api.JobSubmitRequest, _ configuration.SubmissionConfig) error {
	gangDetailsByGangId := make(map[string]schedulercontext.GangInfo)
	for _, job := range request.JobRequestItems {
		actual, err := schedulercontext.GangInfoFromLegacySchedulerJob(jobAdapter{job})
		if err != nil {
			return fmt.Errorf("invalid gang annotations: %s", err.Error())
		}
		if actual.Id == "" {
			continue
		}
		if expected, ok := gangDetailsByGangId[actual.Id]; ok {
			if expected.Cardinality != actual.Cardinality {
				return errors.Errorf(
					"inconsistent gang cardinality in gang %s: expected %d but got %d",
					actual.Id, expected.Cardinality, actual.Cardinality,
				)
			}
			if expected.PriorityClassName != actual.PriorityClassName {
				return errors.Errorf(
					"inconsistent PriorityClassName in gang %s: expected %s but got %s",
					actual.Id, expected.PriorityClassName, actual.PriorityClassName,
				)
			}
			if actual.NodeUniformity != expected.NodeUniformity {
				return errors.Errorf(
					"inconsistent nodeUniformityLabel in gang %s: expected %s but got %s",
					actual.Id, expected.NodeUniformity, actual.NodeUniformity,
				)
			}
		} else {
			gangDetailsByGangId[actual.Id] = actual
		}
	}
	return nil
}

// Validates the job request doesn't define a TerminationGracePeriod outside the range allowed in configuration
func validateTerminationGracePeriod(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}
	if spec.TerminationGracePeriodSeconds != nil && *spec.TerminationGracePeriodSeconds != 0 {
		terminationGracePeriodSeconds := *spec.TerminationGracePeriodSeconds
		if terminationGracePeriodSeconds < int64(config.MinTerminationGracePeriod.Seconds()) ||
			terminationGracePeriodSeconds > int64(config.MaxTerminationGracePeriod.Seconds()) {
			return fmt.Errorf(
				"terminationGracePeriodSeconds of %d must be [%d, %d], or omitted",
				terminationGracePeriodSeconds,
				config.MinTerminationGracePeriod,
				config.MaxTerminationGracePeriod)
		}
	}
	return nil
}

// Ensures that any init containers request non-integer cpu.  This is because when using static cpu manager init
// containers with integer cpu can cause pods to fail with an error of "Pod Allocate failed due to not enough cpus
// available to satisfy request, which is unexpected".
// See https://github.com/kubernetes/kubernetes/issues/112228
func validateInitContainerCpu(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	if !config.AssertInitContainersRequestFractionalCpu {
		return nil
	}

	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}
	const errMsg = "Init container %s contains invalid cpu value %s.  All init containers must set requests/limits to fractional cpu e.g. 900m"
	for _, container := range spec.InitContainers {
		requestCpu := container.Resources.Requests.Cpu()
		limitCpu := container.Resources.Limits.Cpu()
		if !requestCpu.IsZero() && requestCpu.MilliValue()%1000 == 0 {
			return fmt.Errorf(errMsg, container.Name, requestCpu)
		}
		if !limitCpu.IsZero() && limitCpu.MilliValue()%1000 == 0 {
			return fmt.Errorf(errMsg, container.Name, limitCpu)
		}
	}
	return nil
}

func validateTolerations(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()

	if spec == nil {
		return nil
	}

	if len(spec.Tolerations) == 0 || len(config.RestrictedTolerationKeys) == 0 {
		return nil
	}

	jobTolerationKeys := map[string]bool{}
	for _, s := range spec.Tolerations {
		jobTolerationKeys[s.Key] = true
	}

	for _, restricted := range config.RestrictedTolerationKeys {
		if jobTolerationKeys[restricted] {
			return fmt.Errorf("toleration %s is not user settable", restricted)
		}
	}
	return nil
}
