import { LookoutApi, LookoutJobInfo } from '../openapi'
import { getOrDefault } from "../utils";

export type JobInfoViewModel = {
  jobId: string
  queue: string
  owner: string
  jobSet: string
  submissionTime: string
  jobState: string
}

function jobInfoToViewModel(jobInfo: LookoutJobInfo): JobInfoViewModel {
  const jobId = getOrDefault(jobInfo.job?.id, "-")
  const queue = getOrDefault(jobInfo.job?.queue, "-")
  const owner = getOrDefault(jobInfo.job?.owner, "-")
  const jobSet = getOrDefault(jobInfo.job?.jobSetId, "-")
  const submissionTime = getOrDefault(jobInfo.job?.created, new Date()).toLocaleString()
  const jobState = parseJobState(jobInfo)

  return {
    jobId: jobId,
    queue: queue,
    owner: owner,
    jobSet: jobSet,
    submissionTime: submissionTime,
    jobState: jobState,
  }
}

function parseJobState(jobInfo: LookoutJobInfo): string {
  if (jobInfo.cancelled) {
    return "Cancelled"
  }
  if (jobInfo.runs && jobInfo.runs.length > 0) {
    const lastRun = jobInfo.runs[jobInfo.runs.length - 1]
    if (lastRun.finished && lastRun.succeeded) {
      return "Succeeded"
    }
    if (lastRun.finished && !lastRun.succeeded) {
      return "Failed"
    }
    if (lastRun.started) {
      return "Running"
    }
    if (lastRun.created) {
      return "Pending"
    }
    return "Unknown"
  } else {
    return "Queued"
  }
}

export class JobService {

  api: LookoutApi;

  constructor(lookoutAPi: LookoutApi) {
    this.api = lookoutAPi
  }

  getOverview() {
    return this.api.overview()
  }

  async getJobsInQueue(queue: string, take: number, skip: number): Promise<JobInfoViewModel[]> {
    const response = await this.api.getJobsInQueue({
      body: {
        queue: queue,
        take: take,
        skip: skip,
        newestFirst: true,
      }
    });
    if (response.jobInfos) {
      return response.jobInfos.map(jobInfoToViewModel)
    }
    return []
  }
}
