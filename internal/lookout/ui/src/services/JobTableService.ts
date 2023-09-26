import { GetJobsRequest, Job, JobService } from "./JobService"
import { updateArray } from "../utils"

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
    jobStateDuration: "",
    queue: "",
    submissionTime: "",
    runs: [],
    jobYaml: "",
    annotations: {},
    namespace: "",
    loadState: "Loading",
    containers: new Map<number, string[]>(),
  }
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

  async loadJobs(request: GetJobsRequest, start: number, stop: number, signal: AbortSignal | undefined) {
    const startBatch = Math.floor(start / this.batchSize)
    const endBatch = Math.floor(stop / this.batchSize)
    const loadStartIndex = startBatch * this.batchSize
    const loadEndIndex = endBatch * this.batchSize + this.batchSize
    this.markJobsAsLoaded(loadStartIndex, loadEndIndex)

    const newJobsLoaded: JobMetadata[] = []
    let canLoadMore = true
    request.take = this.batchSize

    for (let i = startBatch; i <= endBatch; i++) {
      request.skip = i * this.batchSize
      const [jobsBatch, interrupted] = await this.requestJobs(request, signal)
      if (interrupted) {
        this.jobs = [createLoadingJob()]
        return
      }
      newJobsLoaded.push(...convertToLoaded(jobsBatch))
      if (jobsBatch.length < this.batchSize) {
        canLoadMore = false
      }
    }

    updateArray(this.jobs, newJobsLoaded, loadStartIndex)

    this.largestLoadedIndex = Math.max(this.largestLoadedIndex, loadStartIndex + newJobsLoaded.length)
    this.jobs = this.jobs.slice(0, this.largestLoadedIndex)

    if (canLoadMore) {
      this.jobs.push(createLoadingJob())
    }
  }

  jobIsLoaded(index: number): boolean {
    return index >= 0 && index < this.jobs.length && this.jobs[index].loadState === "Loaded"
  }

  refresh() {
    this.jobs = this.jobs.map(() => createLoadingJob())
    if (this.jobs.length === 0) {
      this.jobs = [createLoadingJob()]
    }
    this.largestLoadedIndex = 0
  }

  private async requestJobs(request: GetJobsRequest, signal: AbortSignal | undefined): Promise<[Job[], boolean]> {
    // Abort previous request
    try {
      const jobs = await this.jobService.getJobs(request, signal)
      return [jobs, false]
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") {
        return Promise.resolve([[], true])
      }
      console.error(e)
      return Promise.resolve([[], false])
    }
  }

  private markJobsAsLoaded(start: number, stop: number) {
    for (let i = start; i <= stop; i++) {
      if (i < this.jobs.length) {
        this.jobs[i].loadState = "Loaded"
      }
    }
  }
}
