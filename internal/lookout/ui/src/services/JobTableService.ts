import { updateArray } from "../utils"
import JobService, { GetJobsRequest, Job } from "./JobService"

type JobLoadState = "Loading" | "Loaded"

type JobMetadata = Job & {
  loadState: JobLoadState
}

function createLoadingJob(): JobMetadata {
  return {
    owner: "",
    jobId: "Loading",
    jobSet: "",
    priority: 0,
    jobState: "",
    queue: "",
    submissionTime: "",
    runs: [],
    jobYaml: "",
    annotations: {},
    namespace: "",
    loadState: "Loading",
  }
}

function jobIsLoaded(job: JobMetadata): boolean {
  return job.loadState === "Loaded"
}

function convertToLoaded(jobs: Job[]): JobMetadata[] {
  return jobs.map((job) => ({
    ...job,
    loadState: "Loaded",
  }))
}

export default class JobTableService {
  jobService: JobService
  batchSize: number

  jobs: JobMetadata[]
  largestLoadedIndex: number

  constructor(jobService: JobService, batchSize: number) {
    this.jobService = jobService
    this.batchSize = batchSize
    this.jobs = [createLoadingJob()]
    this.largestLoadedIndex = 0
  }

  getJobs(): Job[] {
    return this.jobs
  }

  async loadJobs(request: GetJobsRequest, start: number, stop: number) {
    console.log(`Actually loading more jobs ${start} ${stop}`)
    const startBatch = Math.floor(start / this.batchSize)
    const endBatch = Math.floor(stop / this.batchSize)
    const startBatchIndex = startBatch * this.batchSize
    const endBatchIndex = endBatch * this.batchSize + this.batchSize
    this.markJobsAsLoaded(startBatchIndex, endBatchIndex)

    const newJobsLoaded: JobMetadata[] = []
    let canLoadMore = true
    request.take = this.batchSize

    for (let i = startBatch; i <= endBatch; i++) {
      request.skip = i * this.batchSize
      const jobsBatch = await this.jobService.getJobs(request)
      newJobsLoaded.push(...convertToLoaded(jobsBatch))
      if (jobsBatch.length < this.batchSize) {
        canLoadMore = false
      }
    }

    updateArray(this.jobs, newJobsLoaded, startBatchIndex)

    this.largestLoadedIndex = Math.max(this.largestLoadedIndex, startBatchIndex + newJobsLoaded.length)
    this.jobs = this.jobs.slice(0, this.largestLoadedIndex)

    if (canLoadMore) {
      this.jobs.push(createLoadingJob())
    }
  }

  jobIsLoaded(index: number): boolean {
    return index >= 0 && index < this.jobs.length && jobIsLoaded(this.jobs[index])
  }

  refresh() {
    this.jobs = this.jobs.map(() => createLoadingJob())
    if (this.jobs.length === 0) {
      this.jobs = [createLoadingJob()]
    }
    this.largestLoadedIndex = 0
  }

  private markJobsAsLoaded(start: number, stop: number) {
    for (let i = start; i <= stop; i++) {
      if (i < this.jobs.length) {
        this.jobs[i].loadState = "Loaded"
      }
    }
  }
}
