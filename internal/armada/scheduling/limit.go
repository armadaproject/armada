package scheduling

import "github.com/G-Research/armada/pkg/api"

type LeasePayloadLimit struct {
	remainingJobCount              int
	remainingPayloadSizeLimitBytes int
	maxExpectedJobSizeBytes        int
}

// NewLeasePayloadLimit
// numberOfJobsLimit            - This is the maximum number of jobs per lease payload
// payloadSizeLimitBytes        - This is the maximum size of all the Jobs in the lease payload in bytes
// maxExpectedJobSizeBytes      - This is the maximum expected job size in bytes
// It is used as a threshold to determine when adding more jobs would exceed payloadSizeLimitBytes
func NewLeasePayloadLimit(numberOfJobsLimit int, payloadSizeLimitBytes int, maxExpectedJobSizeBytes int) LeasePayloadLimit {
	return LeasePayloadLimit{
		remainingJobCount:              numberOfJobsLimit,
		remainingPayloadSizeLimitBytes: payloadSizeLimitBytes,
		maxExpectedJobSizeBytes:        maxExpectedJobSizeBytes,
	}
}

func (s *LeasePayloadLimit) RemoveFromRemainingLimit(jobs ...*api.Job) {
	for _, job := range jobs {
		s.remainingJobCount -= 1
		s.remainingPayloadSizeLimitBytes -= job.Size()
	}
}

//AtLimit
/*
 This returns true when:
 - remainingJobCount <= 0
 - remainingPayloadSizeLimitBytes <= maxExpectedJobSizeBytes
   If we have less bytes left than the maximum job size, we should consider ourselves at the limit
*/
func (s *LeasePayloadLimit) AtLimit() bool {
	return s.remainingJobCount <= 0 || s.remainingPayloadSizeLimitBytes <= s.maxExpectedJobSizeBytes
}

func (s *LeasePayloadLimit) IsWithinLimit(job *api.Job) bool {
	return s.remainingJobCount >= 1 && s.remainingPayloadSizeLimitBytes-job.Size() >= 0
}
