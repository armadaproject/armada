package retry

import (
	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// extractCondition derives a human-readable condition string from the Error oneof.
// Returns empty string for unrecognized error types.
func extractCondition(err *armadaevents.Error) string {
	if err == nil {
		return ""
	}
	switch reason := err.Reason.(type) {
	case *armadaevents.Error_PodError:
		if reason.PodError == nil {
			return ""
		}
		switch reason.PodError.KubernetesReason {
		case armadaevents.KubernetesReason_OOM:
			return errormatch.ConditionOOMKilled
		case armadaevents.KubernetesReason_Evicted:
			return errormatch.ConditionEvicted
		case armadaevents.KubernetesReason_DeadlineExceeded:
			return errormatch.ConditionDeadlineExceeded
		case armadaevents.KubernetesReason_AppError:
			return errormatch.ConditionAppError
		default:
			return ""
		}
	case *armadaevents.Error_JobRunPreemptedError:
		return errormatch.ConditionPreempted
	case *armadaevents.Error_PodLeaseReturned:
		return errormatch.ConditionLeaseReturned
	default:
		return ""
	}
}

// extractExitCode returns the exit code from FailureInfo if available,
// falling back to the first ContainerError in a PodError.
func extractExitCode(err *armadaevents.Error, fi *armadaevents.FailureInfo) int32 {
	if err == nil {
		return 0
	}
	if fi != nil && fi.ExitCode != 0 {
		return fi.ExitCode
	}
	if pe := err.GetPodError(); pe != nil {
		for _, ce := range pe.ContainerErrors {
			if ce != nil && ce.ExitCode != 0 {
				return ce.ExitCode
			}
		}
	}
	return 0
}

// extractTerminationMessage returns the termination message from FailureInfo if available,
// falling back to the first ContainerError.Message in a PodError.
func extractTerminationMessage(err *armadaevents.Error, fi *armadaevents.FailureInfo) string {
	if err == nil {
		return ""
	}
	if fi != nil && fi.TerminationMessage != "" {
		return fi.TerminationMessage
	}
	if pe := err.GetPodError(); pe != nil {
		for _, ce := range pe.ContainerErrors {
			if ce != nil && ce.Message != "" {
				return ce.Message
			}
		}
	}
	return ""
}

// extractCategories returns category labels from FailureInfo.
func extractCategories(fi *armadaevents.FailureInfo) []string {
	if fi == nil {
		return nil
	}
	return fi.Categories
}
