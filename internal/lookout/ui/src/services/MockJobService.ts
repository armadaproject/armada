import { v4 as uuidv4 } from "uuid"

import { ApiJobState } from "../openapi/armada"
import {
  CancelJobSetsResponse,
  CancelJobsResponse,
  GetJobSetsRequest,
  GetJobsRequest,
  Job,
  JobService,
  JobSet,
  QueueInfo,
  ReprioritizeJobSetsResponse,
  ReprioritizeJobsResponse,
} from "./JobService"

type MockJobServiceConfig = {
  getJobs: {
    delays: {
      [queue: string]: number
    }
    errors: {
      [queue: string]: Error
    }
  }
}

// Class for testing different API call behaviors
export class MockJobService implements JobService {
  config: MockJobServiceConfig

  constructor(config: MockJobServiceConfig) {
    this.config = config
  }

  getOverview(): Promise<QueueInfo[]> {
    return Promise.resolve([])
  }

  // eslint-disable-next-line
  getJobSets(getJobSetsRequest: GetJobSetsRequest): Promise<JobSet[]> {
    return Promise.resolve([])
  }

  async getJobs(getJobsRequest: GetJobsRequest, signal: AbortSignal): Promise<Job[]> {
    if (this.config.getJobs.delays.hasOwnProperty(getJobsRequest.queue)) {
      const interrupted = await sleep(this.config.getJobs.delays[getJobsRequest.queue], signal)
      if (interrupted) {
        throw new DOMException("Aborted", "AbortError")
      }
    }
    if (this.config.getJobs.errors.hasOwnProperty(getJobsRequest.queue)) {
      throw this.config.getJobs.errors[getJobsRequest.queue]
    }
    return Promise.resolve(createJobs(getJobsRequest.queue, getJobsRequest.take))
  }

  // eslint-disable-next-line
  cancelJobs(jobs: Job[]): Promise<CancelJobsResponse> {
    return Promise.resolve({
      cancelledJobs: [],
      failedJobCancellations: [],
    })
  }

  // eslint-disable-next-line
  cancelJobSets(queue: string, jobSets: JobSet[], states: ApiJobState[]): Promise<CancelJobSetsResponse> {
    return Promise.resolve({
      cancelledJobSets: [],
      failedJobSetCancellations: [],
    })
  }

  // eslint-disable-next-line
  reprioritizeJobs(jobs: Job[], newPriority: number): Promise<ReprioritizeJobsResponse> {
    return Promise.resolve({
      reprioritizedJobs: [],
      failedJobReprioritizations: [],
    })
  }

  // eslint-disable-next-line
  reprioritizeJobSets(queue: string, jobSets: JobSet[], newPriority: number): Promise<ReprioritizeJobSetsResponse> {
    return Promise.resolve({
      reprioritizedJobSets: [],
      failedJobSetReprioritizations: [],
    })
  }
}

function sleep(ms: number, signal: AbortSignal | undefined): Promise<boolean> {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(false), ms)
    signal?.addEventListener("abort", () => {
      clearTimeout(timeout)
      resolve(true)
    })
  })
}

function createJobs(queue: string, total: number): Job[] {
  const jobs: Job[] = []

  for (let i = 0; i < total; i++) {
    jobs.push({
      annotations: {},
      containers: new Map<number, string[]>(),
      jobId: uuidv4(),
      jobSet: uuidv4(),
      jobState: "Queued",
      jobYaml: "",
      namespace: queue,
      owner: "anonymous",
      priority: 10,
      queue: queue,
      runs: [],
      submissionTime: new Date().toDateString(),
    })
  }

  return jobs
}
