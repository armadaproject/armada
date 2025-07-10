package main

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/klauspost/compress/zstd"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
	"math/rand"
	"os"
	"sync"
	"time"
)

func saveJobs(jobs []*jobdb.Job, filename string) error {
	start := time.Now()

	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	compressedWriter, err := zstd.NewWriter(f)
	if err != nil {
		return fmt.Errorf("failed to create zstd writer: %w", err)
	}
	defer compressedWriter.Close()

	for _, job := range jobs {
		if err := writeJob(compressedWriter, job); err != nil {
			return fmt.Errorf("failed to write job: %w", err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("Wrote %d jobs to %s in %s\n", len(jobs), filename, elapsed)

	return nil
}

func generateTestJobs(n int) []*jobdb.Job {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	rlf, err := internaltypes.NewResourceListFactory([]configuration.ResourceType{
		{"cpu", k8sResource.MustParse("100m")},
		{"memory", k8sResource.MustParse("1")},
	},
		[]configuration.FloatingResourceConfig{})
	if err != nil {
		panic(err)
	}
	jobDb := jobdb.NewJobDb(map[string]types.PriorityClass{"foo": {
		Priority:    1000,
		Preemptible: true,
	}}, "foo", stringinterner.New(1000), rlf)

	rr1 := v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    k8sResource.MustParse("2"),
			v1.ResourceMemory: k8sResource.MustParse("2Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    k8sResource.MustParse("2"),
			v1.ResourceMemory: k8sResource.MustParse("2Gi"),
		},
	}
	baseJobs := []*jobdb.Job{
		makeBaseJob(jobDb, "queueA", "jobset-alpha", 1, true, false, rr1),
		makeBaseJob(jobDb, "queueB", "jobset-beta", 5, false, true, rr1),
		makeBaseJob(jobDb, "queueC", "jobset-gamma", 10, true, true, rr1),
		makeBaseJob(jobDb, "queueD", "jobset-delta", 20, false, false, rr1),
		makeBaseJob(jobDb, "queueE", "jobset-epsilon", 15, true, false, rr1),
	}

	result := make([]*jobdb.Job, n)
	for i := 0; i < n; i++ {
		idx := r.Intn(len(baseJobs))
		base := baseJobs[idx]
		job, err := jobDb.NewJob(
			util.NewULID(),
			base.Jobset(),
			base.Queue(),
			base.Priority(),
			base.JobSchedulingInfo(),
			base.Queued(),
			1,
			false,
			false,
			false,
			time.Now().UnixNano(),
			base.Validated(),
			[]string{"cpu", "gpu"},
			1)
		if err != nil {
			panic(err)
		}
		result[i] = job
	}
	return result
}

func makeBaseJob(jobDb *jobdb.JobDb, queue, jobset string, priority int, validated, queued bool, rr v1.ResourceRequirements) *jobdb.Job {
	schedulingInfo := &internaltypes.JobSchedulingInfo{
		Lifetime:          3600,
		PriorityClassName: "foo",
		SubmitTime:        time.Now(),
		Priority:          uint32(priority),
		PodRequirements: &internaltypes.PodRequirements{
			NodeSelector: map[string]string{"node": queue},
			Tolerations: []v1.Toleration{{
				Key:      "spot",
				Operator: v1.TolerationOpEqual,
				Value:    "true",
				Effect:   v1.TaintEffectNoSchedule,
			}},
			ResourceRequirements: rr,
		},
		Version: 2,
	}

	job, err := jobDb.NewJob(
		util.NewULID(),
		jobset,
		queue,
		uint32(priority),
		schedulingInfo,
		queued,
		1,
		false,
		false,
		false,
		time.Now().UnixNano(),
		validated,
		[]string{"cpu", "gpu"},
		1)
	if err != nil {
		panic(err)
	}
	return job
}

func main() {
	numJobs := 10000000
	numParts := 8
	jobsPerPart := numJobs / numParts

	fmt.Println("creating jobs")
	jobs := generateTestJobs(numJobs)
	fmt.Println("jobs created")

	fmt.Println("writing jobs in parallel...")
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numParts)

	for i := 0; i < numParts; i++ {
		part := i
		startIdx := part * jobsPerPart
		endIdx := startIdx + jobsPerPart
		jobChunk := jobs[startIdx:endIdx]

		go func() {
			defer wg.Done()
			filename := fmt.Sprintf("jobs.part%d", part)
			err := saveJobs(jobChunk, filename)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error writing %s: %v\n", filename, err)
			} else {
				fmt.Printf("written %s\n", filename)
			}
		}()
	}

	wg.Wait()

	fmt.Printf("All parts written in %s\n", time.Since(start))
}
