package scheduler

import (
	"fmt"
	"strings"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

type invalidGangJobDetails struct {
	jobId  string
	reason string
}

type gangKey struct {
	queue  string
	gangId string
}

type SubmitGangValidator interface {
	Validate(txn *jobdb.Txn, jobs []*jobdb.Job) ([]*invalidGangJobDetails, error)
}

type GangValidator struct{}

func NewGangValidator() *GangValidator {
	return &GangValidator{}
}

func (g *GangValidator) Validate(txn *jobdb.Txn, jobs []*jobdb.Job) ([]*invalidGangJobDetails, error) {
	var result []*invalidGangJobDetails

	for key, gangJobs := range armadaslices.GroupByFunc(
		jobs,
		func(job *jobdb.Job) gangKey {
			return gangKey{queue: job.Queue(), gangId: job.GetGangInfo().Id()}
		},
	) {
		if key.gangId == "" {
			continue
		}

		valid, reason, err := g.validateGang(txn, &key, gangJobs)
		if err != nil {
			return nil, err
		}

		if !valid {
			for _, gangJob := range gangJobs {
				result = append(result, &invalidGangJobDetails{jobId: gangJob.Id(), reason: reason})
			}
		}
	}

	return result, nil
}

func (g *GangValidator) validateGang(txn *jobdb.Txn, key *gangKey, gangJobs []*jobdb.Job) (bool, string, error) {
	if len(gangJobs) == 0 {
		return false, "", fmt.Errorf("no gang jobs supplied for gang %s - %s", key.queue, key.gangId)
	}

	allGangMembersInDb, err := txn.GetGangJobsByGangId(key.queue, key.gangId)
	if err != nil {
		return false, "", err
	}
	if len(allGangMembersInDb) == 0 || len(allGangMembersInDb) < len(gangJobs) {
		return false, "", fmt.Errorf("jobs being validated missing from job db")
	}

	jobsByGangUniqueGangInfo := armadaslices.GroupByFunc(
		allGangMembersInDb,
		func(job *jobdb.Job) jobdb.GangInfo {
			return job.GetGangInfo()
		},
	)

	// Validate all gang members are queued and have consistent priority class
	representativeJob := allGangMembersInDb[0]
	for _, gangJob := range allGangMembersInDb {
		if !gangJob.Queued() {
			reason := fmt.Sprintf("cannot submit to gang that has running jobs - example running job %s", gangJob.Id())
			return false, reason, nil
		}
		if representativeJob.PriorityClassName() != gangJob.PriorityClassName() {
			reason := fmt.Sprintf("cannot submit jobs with different priority classes to the same gang - job %s has priority class %s but job %s has %s",
				representativeJob.Id(), representativeJob.PriorityClassName(), gangJob.Id(), gangJob.PriorityClassName())
			return false, reason, nil
		}
	}

	if len(jobsByGangUniqueGangInfo) > 1 {
		reason := fmt.Sprintf("cannot submit jobs with different gang info with the same gan id, found %d unique sets of gang info for gang id %s\n%s",
			len(jobsByGangUniqueGangInfo), key.gangId, createGangInfoSummaryString(jobsByGangUniqueGangInfo))
		return false, reason, nil
	}

	if len(allGangMembersInDb) > representativeJob.GetGangInfo().Cardinality() {
		reason := fmt.Sprintf("cannot submit more jobs to gang than specified in gang cardinality - cardinality set to %d (based on job %s) but found %d jobs",
			gangJobs[0].GetGangInfo().Cardinality(), representativeJob.Id(), len(gangJobs))
		return false, reason, nil
	}

	return true, "", nil
}

func createGangInfoSummaryString(gangInfos map[jobdb.GangInfo][]*jobdb.Job) string {
	summary := "details:\n"
	count := 0
	for info, jobs := range gangInfos {
		summary = summary + fmt.Sprintf("gang (%s) - number of jobs %d - example id %s\n", info.String(), len(jobs), strings.ToLower(jobs[0].Id()))
		count++
		// Limit the error message to a sensible length
		if count >= 10 {
			summary = summary + "<truncated>"
			break
		}
	}
	return summary
}
