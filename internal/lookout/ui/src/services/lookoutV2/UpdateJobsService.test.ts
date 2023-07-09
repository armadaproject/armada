import _ from "lodash"

import { createJobBatches } from "./UpdateJobsService"
import { Job, JobId, JobState } from "../../models/lookoutV2Models"

describe("UpdateJobsService", () => {
  describe("createJobBatches", () => {
    it("gives an empty map if there are no jobs", () => {
      const result = createJobBatches([], 100)
      expect(result).toEqual(new Map<string, Map<string, JobId[][]>>())
    })
  })

  describe("splits jobs by queue and jobset", () => {
    const jobArrs = [
      createManyJobs("queue-1", "job-set-1-1", 10),
      createManyJobs("queue-1", "job-set-1-2", 10),
      createManyJobs("queue-2", "job-set-2-1", 10),
      createManyJobs("queue-3", "job-set-3-1", 10),
      createManyJobs("queue-3", "job-set-3-2", 10),
    ]
    const jobs = ([] as Job[]).concat(...jobArrs)
    const result = createJobBatches(jobs, 100)
    expect(result).toEqual(
      new Map<string, Map<string, JobId[][]>>([
        [
          "queue-1",
          new Map<string, JobId[][]>([
            ["job-set-1-1", [extractIds(jobArrs[0])]],
            ["job-set-1-2", [extractIds(jobArrs[1])]],
          ]),
        ],
        ["queue-2", new Map<string, JobId[][]>([["job-set-2-1", [extractIds(jobArrs[2])]]])],
        [
          "queue-3",
          new Map<string, JobId[][]>([
            ["job-set-3-1", [extractIds(jobArrs[3])]],
            ["job-set-3-2", [extractIds(jobArrs[4])]],
          ]),
        ],
      ]),
    )
  })

  describe("splits jobs by queue and jobset and batches", () => {
    const jobArrs = [
      createManyJobs("queue-1", "job-set-1-1", 99),
      createManyJobs("queue-1", "job-set-1-2", 101),
      createManyJobs("queue-2", "job-set-2-1", 100),
      createManyJobs("queue-3", "job-set-3-1", 100),
      createManyJobs("queue-3", "job-set-3-2", 100),
    ]
    const jobs = ([] as Job[]).concat(...jobArrs)
    const batchSize = 10
    const result = createJobBatches(jobs, 10)
    const getBatches = (jobs: Job[]) => _.chunk(jobs, batchSize).map(extractIds)
    expect(result).toEqual(
      new Map<string, Map<string, JobId[][]>>([
        [
          "queue-1",
          new Map<string, JobId[][]>([
            ["job-set-1-1", getBatches(jobArrs[0])],
            ["job-set-1-2", getBatches(jobArrs[1])],
          ]),
        ],
        ["queue-2", new Map<string, JobId[][]>([["job-set-2-1", getBatches(jobArrs[2])]])],
        [
          "queue-3",
          new Map<string, JobId[][]>([
            ["job-set-3-1", getBatches(jobArrs[3])],
            ["job-set-3-2", getBatches(jobArrs[4])],
          ]),
        ],
      ]),
    )
  })
})

function extractIds(jobs: Job[]): JobId[] {
  return jobs.map((job) => job.jobId)
}

function createManyJobs(queue: string, jobSet: string, n: number): Job[] {
  const result: Job[] = []
  for (let i = 0; i < n; i++) {
    result.push({
      queue: queue,
      jobSet: jobSet,
      jobId: `${queue}-${jobSet}-${i}`,
      annotations: {},
      cpu: 0,
      ephemeralStorage: 0,
      gpu: 0,
      lastTransitionTime: "",
      memory: 0,
      owner: "",
      priority: 0,
      runs: [],
      state: JobState.Running,
      submitted: "",
      priorityClass: "armada-preemptible",
    })
  }
  return result
}
