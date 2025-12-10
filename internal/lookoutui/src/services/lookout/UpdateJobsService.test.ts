import _ from "lodash"

import { Job, JobId, JobState } from "../../models/lookoutModels"

import { createJobBatches } from "./UpdateJobsService"

describe("UpdateJobsService", () => {
  describe("createJobBatches", () => {
    it("gives an empty map if there are no jobs", () => {
      const result = createJobBatches([], 100)
      expect(result).toEqual(new Map<string, Map<string, JobId[][]>>())
    })
  })

  it("splits jobs by queue and job set", () => {
    const jobArrays = [
      createManyJobs("queue-1", "job-set-1-1", 10),
      createManyJobs("queue-1", "job-set-1-2", 10),
      createManyJobs("queue-2", "job-set-2-1", 10),
      createManyJobs("queue-3", "job-set-3-1", 10),
      createManyJobs("queue-3", "job-set-3-2", 10),
    ]
    const jobs = ([] as Job[]).concat(...jobArrays)
    const result = createJobBatches(jobs, 100)
    expect(result).toEqual(
      new Map<string, Map<string, JobId[][]>>([
        [
          "queue-1",
          new Map<string, JobId[][]>([
            ["job-set-1-1", [extractIds(jobArrays[0])]],
            ["job-set-1-2", [extractIds(jobArrays[1])]],
          ]),
        ],
        ["queue-2", new Map<string, JobId[][]>([["job-set-2-1", [extractIds(jobArrays[2])]]])],
        [
          "queue-3",
          new Map<string, JobId[][]>([
            ["job-set-3-1", [extractIds(jobArrays[3])]],
            ["job-set-3-2", [extractIds(jobArrays[4])]],
          ]),
        ],
      ]),
    )
  })

  it("splits jobs by queue and job set and batches", () => {
    const jobArrays = [
      createManyJobs("queue-1", "job-set-1-1", 99),
      createManyJobs("queue-1", "job-set-1-2", 101),
      createManyJobs("queue-2", "job-set-2-1", 100),
      createManyJobs("queue-3", "job-set-3-1", 100),
      createManyJobs("queue-3", "job-set-3-2", 100),
    ]
    const jobs = ([] as Job[]).concat(...jobArrays)
    const batchSize = 10
    const result = createJobBatches(jobs, 10)
    const getBatches = (jobs: Job[]) => _.chunk(jobs, batchSize).map(extractIds)
    expect(result).toEqual(
      new Map<string, Map<string, JobId[][]>>([
        [
          "queue-1",
          new Map<string, JobId[][]>([
            ["job-set-1-1", getBatches(jobArrays[0])],
            ["job-set-1-2", getBatches(jobArrays[1])],
          ]),
        ],
        ["queue-2", new Map<string, JobId[][]>([["job-set-2-1", getBatches(jobArrays[2])]])],
        [
          "queue-3",
          new Map<string, JobId[][]>([
            ["job-set-3-1", getBatches(jobArrays[3])],
            ["job-set-3-2", getBatches(jobArrays[4])],
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
      namespace: "",
      priority: 0,
      runs: [],
      state: JobState.Running,
      submitted: "",
      priorityClass: "armada-preemptible",
    })
  }
  return result
}
