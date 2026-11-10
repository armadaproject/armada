package armadaevents

// IsEmpty reports whether nothing could be determined about the pod's start up. Every field is
// nullable precisely so that "not measurable" is distinct from zero, which means a message can come
// back with all of them unset - from a pod that reported no conditions, no start time and no
// container statuses. Callers use this to leave start_up_info off the event entirely rather than
// attach a message that says nothing.
//
// CreatedAt is not checked: it comes from the pod's own creationTimestamp and so is set even for a
// pod that reported nothing else, which would make every record look non-empty.
func (m *PodStartUpInfo) IsEmpty() bool {
	if m == nil {
		return true
	}
	return m.ScheduledAt == nil &&
		m.KubeletAcknowledgedAt == nil &&
		m.SandboxReadyAt == nil &&
		m.StartedAt == nil &&
		len(m.InitContainers) == 0 &&
		len(m.Containers) == 0 &&
		len(m.ExtraSteps) == 0
}
