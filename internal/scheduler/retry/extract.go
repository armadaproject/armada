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

// extractExitCode returns the exit code from the first ContainerError in a PodError.
func extractExitCode(err *armadaevents.Error) int32 {
	if err == nil {
		return 0
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

// extractTerminationMessage returns the termination message from the first ContainerError in a PodError.
func extractTerminationMessage(err *armadaevents.Error) string {
	if err == nil {
		return ""
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

// extractCategory returns the failure category from the Error proto.
func extractCategory(err *armadaevents.Error) string {
	if err == nil {
		return ""
	}
	return err.GetFailureCategory()
}

// extractSubcategory returns the failure subcategory from the Error proto.
func extractSubcategory(err *armadaevents.Error) string {
	if err == nil {
		return ""
	}
	return err.GetFailureSubcategory()
}
