import yaml from "js-yaml"

import { SubmitApi } from "../openapi/armada"
import {
  LookoutApi,
  LookoutDurationStats,
  LookoutJobInfo,
  LookoutJobSetInfo,
  LookoutQueueInfo,
  LookoutRunInfo,
  ApiJob,
} from "../openapi/lookout"
import { reverseMap, secondsToDurationString, getErrorMessage } from "../utils"

type DurationFromApi = {
  seconds?: number
  nanos?: number
}

export type QueueInfo = {
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  oldestQueuedJob?: Job
  longestRunningJob?: Job
  oldestQueuedDuration: string
  longestRunningDuration: string
}

export type JobSet = {
  jobSetId: string
  queue: string
  jobsQueued: number
  jobsPending: number
  jobsRunning: number
  jobsSucceeded: number
  jobsFailed: number
  latestSubmissionTime: string

  runningStats?: DurationStats
  queuedStats?: DurationStats
}

export type DurationStats = {
  shortest: number
  longest: number
  average: number
  median: number
  q1: number
  q3: number
}

export interface GetJobsRequest {
  queue: string
  take: number
  skip: number
  jobSets: string[]
  newestFirst: boolean
  jobStates: string[]
  jobId: string
  owner: string
  annotations: { [key: string]: string }
}

export interface GetJobSetsRequest {
  queue: string
  newestFirst: boolean
  activeOnly: boolean
}

export type Job = {
  jobId: string
  queue: string
  owner: string
  jobSet: string
  priority: number
  submissionTime: string
  cancelledTime?: string
  jobState: string
  runs: Run[]
  jobYaml: string
  annotations: { [key: string]: string }
  namespace: string
}

export type Run = {
  k8sId: string
  cluster: string
  node?: string
  succeeded: boolean
  error?: string
  podCreationTime?: string
  podStartTime?: string
  finishTime?: string
  podNumber: number
  containers: string[]
}

export type CancelJobsResult = {
  cancelledJobs: Job[]
  failedJobCancellations: FailedJobCancellation[]
}

export type CancelJobSetsResult = {
  cancelledJobSets: JobSet[]
  failedJobSetCancellations: {
    jobSet: JobSet
    error: string
  }[]
}

export type FailedJobCancellation = {
  job: Job
  error: string
}

export type ReprioritizeJobsResult = {
  reprioritizedJobs: Job[]
  failedJobReprioritizations: FailedJobReprioritizations[]
}

export type ReprioritizeJobSetsResult = {
  reprioritizedJobSets: JobSet[]
  failedJobSetReprioritizations: {
    jobSet: JobSet
    error: string
  }[]
}

export type FailedJobReprioritizations = {
  job: Job
  error: string
}

const JOB_STATE_MAP = new Map<string, string>()
JOB_STATE_MAP.set("QUEUED", "Queued")
JOB_STATE_MAP.set("PENDING", "Pending")
JOB_STATE_MAP.set("RUNNING", "Running")
JOB_STATE_MAP.set("SUCCEEDED", "Succeeded")
JOB_STATE_MAP.set("FAILED", "Failed")
JOB_STATE_MAP.set("CANCELLED", "Cancelled")

const INVERSE_JOB_STATE_MAP = reverseMap(JOB_STATE_MAP)

export const JOB_STATES_FOR_DISPLAY = ["Queued", "Pending", "Running", "Succeeded", "Failed", "Cancelled"]

export default class JobService {
  lookoutApi: LookoutApi
  submitApi: SubmitApi
  userAnnotationPrefix: string

  constructor(lookoutAPi: LookoutApi, submitApi: SubmitApi, userAnnotationPrefix: string) {
    this.lookoutApi = lookoutAPi
    this.submitApi = submitApi
    this.userAnnotationPrefix = userAnnotationPrefix
  }

  async getOverview(): Promise<QueueInfo[]> {
    const queueInfosFromApi = await this.lookoutApi.overview()
    if (!queueInfosFromApi.queues) {
      return []
    }

    return queueInfosFromApi.queues.map((queueInfo) => this.queueInfoToViewModel(queueInfo))
  }

  async getJobSets(getJobSetsRequest: GetJobSetsRequest): Promise<JobSet[]> {
    const jobSetsFromApi = await this.lookoutApi.getJobSets({
      body: {
        queue: getJobSetsRequest.queue,
        newestFirst: getJobSetsRequest.newestFirst,
        activeOnly: getJobSetsRequest.activeOnly,
      },
    })
    if (!jobSetsFromApi.jobSetInfos) {
      return []
    }

    return jobSetsFromApi.jobSetInfos.map(jobSetToViewModel)
  }

  async getJobs(getJobsRequest: GetJobsRequest): Promise<Job[]> {
    const jobStatesForApi = getJobsRequest.jobStates.map(getJobStateForApi)
    const jobSetsForApi = getJobsRequest.jobSets.map(escapeBackslashes)
    try {
      const response = await this.lookoutApi.getJobs({
        body: {
          queue: getJobsRequest.queue,
          take: getJobsRequest.take,
          skip: getJobsRequest.skip,
          jobSetIds: jobSetsForApi,
          newestFirst: getJobsRequest.newestFirst,
          jobStates: jobStatesForApi,
          jobId: getJobsRequest.jobId,
          owner: getJobsRequest.owner,
          userAnnotations: getJobsRequest.annotations,
        },
      })
      if (response.jobInfos) {
        return response.jobInfos.map((jobInfo) => this.jobInfoToViewModel(jobInfo))
      }
    } catch (e) {
      console.error(await e.json())
    }
    return []
  }

  async cancelJobs(jobs: Job[]): Promise<CancelJobsResult> {
    const result: CancelJobsResult = { cancelledJobs: [], failedJobCancellations: [] }
    for (const job of jobs) {
      try {
        const apiResult = await this.submitApi.cancelJobs({
          body: {
            jobId: job.jobId,
          },
        })

        if (!apiResult.cancelledIds || apiResult.cancelledIds.length !== 1 || apiResult.cancelledIds[0] !== job.jobId) {
          result.failedJobCancellations.push({ job: job, error: "No job was cancelled" })
        } else {
          result.cancelledJobs.push(job)
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        result.failedJobCancellations.push({ job: job, error: text })
      }
    }
    return result
  }

  async cancelJobSets(queue: string, jobSets: JobSet[]): Promise<CancelJobSetsResult> {
    const result: CancelJobSetsResult = { cancelledJobSets: [], failedJobSetCancellations: [] }
    for (const jobSet of jobSets) {
      try {
        const apiResult = await this.submitApi.cancelJobs({
          body: {
            queue: queue,
            jobSetId: jobSet.jobSetId,
          },
        })

        if (apiResult.cancelledIds?.length) {
          result.cancelledJobSets.push(jobSet)
        } else {
          result.failedJobSetCancellations.push({ jobSet: jobSet, error: "No job was cancelled" })
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        result.failedJobSetCancellations.push({ jobSet: jobSet, error: text })
      }
    }
    return result
  }

  async reprioritizeJobs(jobs: Job[], newPriority: number): Promise<ReprioritizeJobsResult> {
    const result: ReprioritizeJobsResult = { reprioritizedJobs: [], failedJobReprioritizations: [] }
    const jobIds: string[] = []
    for (const job of jobs) {
      jobIds.push(job.jobId)
    }
    try {
      const apiResult = await this.submitApi.reprioritizeJobs({
        body: {
          jobIds: jobIds,
          newPriority: newPriority,
        },
      })

      if (apiResult == null || apiResult.reprioritizationResults == null) {
        const errorMessage = "No reprioritizationResults found in response body"
        console.error(errorMessage)
        for (const job of jobs) {
          result.failedJobReprioritizations.push({ job: job, error: errorMessage })
        }
      } else {
        for (const job of jobs) {
          if (apiResult.reprioritizationResults?.hasOwnProperty(job.jobId)) {
            const error = apiResult.reprioritizationResults[job.jobId]
            if (error === "") {
              result.reprioritizedJobs.push(job)
            } else {
              result.failedJobReprioritizations.push({ job: job, error: error })
            }
          } else {
            result.reprioritizedJobs.push(job)
          }
        }
      }
    } catch (e) {
      console.error(e)

      const errorMessage = await getErrorMessage(e)
      for (const job of jobs) {
        result.failedJobReprioritizations.push({ job: job, error: errorMessage })
      }
    }
    return result
  }

  async reprioritizeJobSets(queue: string, jobSets: JobSet[], newPriority: number): Promise<ReprioritizeJobSetsResult> {
    const result: ReprioritizeJobSetsResult = { reprioritizedJobSets: [], failedJobSetReprioritizations: [] }

    for (const jobSet of jobSets) {
      try {
        const apiResult = await this.submitApi.reprioritizeJobs({
          body: {
            queue: queue,
            jobSetId: jobSet.jobSetId,
            newPriority: newPriority,
          },
        })
        if (apiResult == null || apiResult.reprioritizationResults == null) {
          const errorMessage = "No reprioritizationResults found in response body"
          console.error(errorMessage)
          result.failedJobSetReprioritizations.push({
            jobSet: jobSet,
            error: "No reprioritizationResults found in response body",
          })
          continue
        }

        let errorCount = 0
        let successCount = 0
        let error = ""
        for (const e of Object.values(apiResult.reprioritizationResults)) {
          if (e !== "") {
            errorCount++
            error = e
          } else {
            successCount++
          }
        }
        if (errorCount === 0) {
          result.reprioritizedJobSets.push(jobSet)
        } else {
          const message: string = "Reprioritised: " + successCount + " Failed: " + errorCount + " Reason: " + error
          result.failedJobSetReprioritizations.push({ jobSet: jobSet, error: message })
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        result.failedJobSetReprioritizations.push({ jobSet: jobSet, error: text })
      }
    }
    return result
  }

  private queueInfoToViewModel(queueInfo: LookoutQueueInfo): QueueInfo {
    let oldestQueuedJob: Job | undefined
    let oldestQueuedDuration = "-"
    if (queueInfo.oldestQueuedJob) {
      oldestQueuedJob = this.jobInfoToViewModel(queueInfo.oldestQueuedJob)
    }
    if (queueInfo.oldestQueuedDuration) {
      oldestQueuedDuration = getDurationString(queueInfo.oldestQueuedDuration)
    }

    let longestRunningJob: Job | undefined
    let longestRunningJobDuration = "-"
    if (queueInfo.longestRunningJob) {
      longestRunningJob = this.jobInfoToViewModel(queueInfo.longestRunningJob)
    }
    if (queueInfo.longestRunningDuration) {
      longestRunningJobDuration = getDurationString(queueInfo.longestRunningDuration)
    }

    return {
      queue: queueInfo.queue ?? "Unknown queue",
      jobsQueued: queueInfo.jobsQueued ?? 0,
      jobsPending: queueInfo.jobsPending ?? 0,
      jobsRunning: queueInfo.jobsRunning ?? 0,
      oldestQueuedJob: oldestQueuedJob,
      longestRunningJob: longestRunningJob,
      oldestQueuedDuration: oldestQueuedDuration,
      longestRunningDuration: longestRunningJobDuration,
    }
  }

  private jobInfoToViewModel(jobInfo: LookoutJobInfo): Job {
    const jobId = jobInfo.job?.id ?? "-"
    const queue = jobInfo.job?.queue ?? "-"
    const owner = jobInfo.job?.owner ?? "-"
    const jobSet = jobInfo.job?.jobSetId ?? "-"
    const priority = jobInfo.job?.priority ?? 0
    const submissionTime = dateToString(jobInfo.job?.created ?? new Date())
    const cancelledTime = jobInfo.cancelled ? dateToString(jobInfo.cancelled) : undefined
    const jobState = JOB_STATE_MAP.get(jobInfo.jobState ?? "") ?? "Unknown"
    const jobFromJson = jobInfo.jobJson ? (JSON.parse(jobInfo.jobJson) as ApiJob) : undefined
    const jobYaml = jobInfo.jobJson ? jobJsonToYaml(jobFromJson) : ""
    const runs = getRuns(jobInfo, jobFromJson)
    const annotations = jobInfo.job?.annotations ? this.getAnnotations(jobInfo.job?.annotations) : {}
    const namespace = jobFromJson?.namespace ?? ""

    return {
      jobId: jobId,
      queue: queue,
      owner: owner,
      jobSet: jobSet,
      priority: priority,
      submissionTime: submissionTime,
      cancelledTime: cancelledTime,
      jobState: jobState,
      runs: runs,
      jobYaml: jobYaml,
      annotations: annotations,
      namespace: namespace,
    }
  }

  private getAnnotations(annotations: { [key: string]: string }): { [key: string]: string } {
    const userAnnotations: { [key: string]: string } = {}
    for (const key in annotations) {
      if (key.startsWith(this.userAnnotationPrefix)) {
        userAnnotations[key.substring(this.userAnnotationPrefix.length)] = annotations[key]
      }
    }
    return userAnnotations
  }
}

function escapeBackslashes(str: string) {
  return str.split("\\").join("\\\\")
}

function jobSetToViewModel(jobSet: LookoutJobSetInfo): JobSet {
  return {
    jobSetId: jobSet.jobSet ?? "Unknown job set",
    queue: jobSet.queue ?? "Unknown queue",
    jobsQueued: jobSet.jobsQueued ?? 0,
    jobsPending: jobSet.jobsPending ?? 0,
    jobsRunning: jobSet.jobsRunning ?? 0,
    jobsSucceeded: jobSet.jobsSucceeded ?? 0,
    jobsFailed: jobSet.jobsFailed ?? 0,
    latestSubmissionTime: dateToString(jobSet.submitted ?? new Date()),
    runningStats: durationStatsToViewModel(jobSet.runningStats),
    queuedStats: durationStatsToViewModel(jobSet.queuedStats),
  }
}

function durationStatsToViewModel(durationStats?: LookoutDurationStats): DurationStats | undefined {
  if (
    !(
      durationStats &&
      durationStats.shortest &&
      durationStats.longest &&
      durationStats.average &&
      durationStats.median &&
      durationStats.q1 &&
      durationStats.q3
    )
  ) {
    return undefined
  }

  return {
    shortest: getDurationSeconds(durationStats.shortest),
    longest: getDurationSeconds(durationStats.longest),
    average: getDurationSeconds(durationStats.average),
    median: getDurationSeconds(durationStats.median),
    q1: getDurationSeconds(durationStats.q1),
    q3: getDurationSeconds(durationStats.q3),
  }
}

function getDurationString(durationFromApi: any): string {
  const totalSeconds = getDurationSeconds(durationFromApi)
  return secondsToDurationString(totalSeconds)
}

function getDurationSeconds(durationFromApi: any): number {
  durationFromApi = durationFromApi as DurationFromApi
  let totalSeconds = 0

  if (durationFromApi.seconds) {
    totalSeconds += durationFromApi.seconds
  }

  if (durationFromApi.nanos) {
    totalSeconds += durationFromApi.nanos / 1e9
  }

  return totalSeconds
}

function dateToString(date: Date): string {
  return date.toLocaleString("en-GB", {
    timeZone: "UTC",
    timeZoneName: "short",
  })
}

function getRuns(jobInfo: LookoutJobInfo, jobFromJson: ApiJob | undefined): Run[] {
  if (!jobInfo.runs || jobInfo.runs.length === 0) {
    return []
  }

  sortRuns(jobInfo.runs)
  return jobInfo.runs.map((r) => runInfoToViewModel(r, jobFromJson))
}

function sortRuns(runs: LookoutRunInfo[]) {
  runs.sort((runA, runB) => {
    const latestA = getLatestTimeForRun(runA)
    const latestB = getLatestTimeForRun(runB)
    return latestA - latestB
  })
}

function getLatestTimeForRun(run: LookoutRunInfo) {
  return Math.max(
    run.created?.getTime() ?? -Infinity,
    run.started?.getTime() ?? -Infinity,
    run.finished?.getTime() ?? -Infinity,
  )
}

function runInfoToViewModel(run: LookoutRunInfo, job: ApiJob | undefined): Run {
  const podSpecs = job?.podSpecs ?? []
  const containers = podSpecs.length > 0 ? podSpecs[run.podNumber ?? 0].containers ?? [] : []
  const containerNames = containers.length > 0 ? containers.map((c) => c.name ?? "") : [""]
  return {
    k8sId: run.k8sId ?? "Unknown Kubernetes id",
    cluster: run.cluster ?? "Unknown cluster",
    node: run.node,
    succeeded: run.succeeded ?? false,
    error: run.error,
    podCreationTime: run.created ? dateToString(run.created) : undefined,
    podStartTime: run.started ? dateToString(run.started) : undefined,
    finishTime: run.finished ? dateToString(run.finished) : undefined,
    podNumber: run.podNumber ?? 0,
    containers: containerNames,
  }
}

function jobJsonToYaml(jobJson: ApiJob | undefined): string {
  return jobJson ? yaml.dump(jobJson) : ""
}

function getJobStateForApi(displayedJobState: string): string {
  const jobState = INVERSE_JOB_STATE_MAP.get(displayedJobState)
  if (!jobState) {
    throw new Error(`Unrecognized job state: "${displayedJobState}"`)
  }
  return jobState
}
