import yaml from "js-yaml"

import { ApiJobState, SubmitApi } from "../openapi/armada"
import {
  LookoutApi,
  LookoutDurationStats,
  LookoutJobInfo,
  LookoutJobSetInfo,
  LookoutQueueInfo,
  LookoutRunInfo,
  ApiJob,
  V1Container,
  V1PodSpec,
} from "../openapi/lookout"
import { inverseMap, secondsToDurationString, getErrorMessage } from "../utils"

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
  jobsCancelled: number
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
  jobStateDuration: string
  runs: Run[]
  jobYaml: string
  annotations: { [key: string]: string }
  namespace: string
  containers: Map<number, string[]> // Map from pod number to containers in that pod
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
  preemptedTime?: string
  podNumber: number
}

export type CancelJobsResponse = {
  cancelledJobs: Job[]
  failedJobCancellations: {
    job: Job
    error: string
  }[]
}

export type CancelJobSetsResponse = {
  cancelledJobSets: JobSet[]
  failedJobSetCancellations: {
    jobSet: JobSet
    error: string
  }[]
}

export type ReprioritizeJobsResponse = {
  reprioritizedJobs: Job[]
  failedJobReprioritizations: {
    job: Job
    error: string
  }[]
}

export type ReprioritizeJobSetsResponse = {
  reprioritizedJobSets: JobSet[]
  failedJobSetReprioritizations: {
    jobSet: JobSet
    error: string
  }[]
}

const JOB_STATE_MAP = new Map<string, string>()
JOB_STATE_MAP.set("QUEUED", "Queued")
JOB_STATE_MAP.set("PENDING", "Pending")
JOB_STATE_MAP.set("RUNNING", "Running")
JOB_STATE_MAP.set("SUCCEEDED", "Succeeded")
JOB_STATE_MAP.set("FAILED", "Failed")
JOB_STATE_MAP.set("CANCELLED", "Cancelled")

const INVERSE_JOB_STATE_MAP = inverseMap(JOB_STATE_MAP)

export const JOB_STATES_FOR_DISPLAY = ["Queued", "Pending", "Running", "Succeeded", "Failed", "Cancelled"]

export const UNKNOWN_CONTAINER = "Unknown Container"

export interface JobService {
  getOverview(): Promise<QueueInfo[]>

  getJobSets(getJobSetsRequest: GetJobSetsRequest): Promise<JobSet[]>

  getJobs(getJobsRequest: GetJobsRequest, signal: AbortSignal | undefined): Promise<Job[]>

  cancelJobs(jobs: Job[]): Promise<CancelJobsResponse>

  cancelJobSets(queue: string, jobSets: JobSet[], states: ApiJobState[]): Promise<CancelJobSetsResponse>

  reprioritizeJobs(jobs: Job[], newPriority: number): Promise<ReprioritizeJobsResponse>

  reprioritizeJobSets(queue: string, jobSets: JobSet[], newPriority: number): Promise<ReprioritizeJobSetsResponse>
}

export class LookoutJobService implements JobService {
  lookoutApi: LookoutApi
  submitApi: SubmitApi
  userAnnotationPrefix: string

  constructor(lookoutApi: LookoutApi, submitApi: SubmitApi, userAnnotationPrefix: string) {
    this.lookoutApi = lookoutApi
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

  async getJobs(getJobsRequest: GetJobsRequest, signal: AbortSignal | undefined): Promise<Job[]> {
    const jobStatesForApi = getJobsRequest.jobStates.map(getJobStateForApi)
    const jobSetsForApi = getJobsRequest.jobSets
    const response = await this.lookoutApi.getJobs(
      {
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
      },
      { signal },
    )
    if (response.jobInfos) {
      return response.jobInfos.map((jobInfo) => this.jobInfoToViewModel(jobInfo))
    }
    return []
  }

  async cancelJobs(jobs: Job[]): Promise<CancelJobsResponse> {
    const response: CancelJobsResponse = { cancelledJobs: [], failedJobCancellations: [] }
    for (const job of jobs) {
      try {
        const apiResponse = await this.submitApi.cancelJobs({
          body: {
            jobId: job.jobId,
          },
        })

        if (
          !apiResponse.cancelledIds ||
          apiResponse.cancelledIds.length !== 1 ||
          apiResponse.cancelledIds[0] !== job.jobId
        ) {
          response.failedJobCancellations.push({ job: job, error: "No job was cancelled" })
        } else {
          response.cancelledJobs.push(job)
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        response.failedJobCancellations.push({ job: job, error: text })
      }
    }
    return response
  }

  async cancelJobSets(queue: string, jobSets: JobSet[], states: ApiJobState[]): Promise<CancelJobSetsResponse> {
    const response: CancelJobSetsResponse = { cancelledJobSets: [], failedJobSetCancellations: [] }
    for (const jobSet of jobSets) {
      try {
        await this.submitApi.cancelJobSet({
          body: {
            queue: queue,
            jobSetId: jobSet.jobSetId,
            filter: {
              states: states,
            },
          },
        })
        response.cancelledJobSets.push(jobSet)
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        response.failedJobSetCancellations.push({ jobSet: jobSet, error: text })
      }
    }
    return response
  }

  async reprioritizeJobs(jobs: Job[], newPriority: number): Promise<ReprioritizeJobsResponse> {
    const response: ReprioritizeJobsResponse = { reprioritizedJobs: [], failedJobReprioritizations: [] }
    const jobIds: string[] = []
    for (const job of jobs) {
      jobIds.push(job.jobId)
    }
    try {
      const apiResponse = await this.submitApi.reprioritizeJobs({
        body: {
          jobIds: jobIds,
          newPriority: newPriority,
        },
      })

      if (apiResponse == null || apiResponse.reprioritizationResults == null) {
        const errorMessage = "No reprioritization results found in response body"
        console.error(errorMessage)
        for (const job of jobs) {
          response.failedJobReprioritizations.push({ job: job, error: errorMessage })
        }
      } else {
        for (const job of jobs) {
          if (apiResponse.reprioritizationResults?.hasOwnProperty(job.jobId)) {
            const error = apiResponse.reprioritizationResults[job.jobId]
            if (error === "") {
              response.reprioritizedJobs.push(job)
            } else {
              response.failedJobReprioritizations.push({ job: job, error: error })
            }
          } else {
            response.reprioritizedJobs.push(job)
          }
        }
      }
    } catch (e) {
      console.error(e)

      const errorMessage = await getErrorMessage(e)
      for (const job of jobs) {
        response.failedJobReprioritizations.push({ job: job, error: errorMessage })
      }
    }
    return response
  }

  async reprioritizeJobSets(
    queue: string,
    jobSets: JobSet[],
    newPriority: number,
  ): Promise<ReprioritizeJobSetsResponse> {
    const response: ReprioritizeJobSetsResponse = { reprioritizedJobSets: [], failedJobSetReprioritizations: [] }

    for (const jobSet of jobSets) {
      try {
        const apiResponse = await this.submitApi.reprioritizeJobs({
          body: {
            queue: queue,
            jobSetId: jobSet.jobSetId,
            newPriority: newPriority,
          },
        })
        if (apiResponse == null || apiResponse.reprioritizationResults == null) {
          const errorMessage = "No reprioritizationResults found in response body"
          console.error(errorMessage)
          response.failedJobSetReprioritizations.push({
            jobSet: jobSet,
            error: "No reprioritizationResults found in response body",
          })
          continue
        }

        let errorCount = 0
        let successCount = 0
        let error = ""
        for (const e of Object.values(apiResponse.reprioritizationResults)) {
          if (e !== "") {
            errorCount++
            error = e
          } else {
            successCount++
          }
        }
        if (errorCount === 0) {
          response.reprioritizedJobSets.push(jobSet)
        } else {
          const message = `Reprioritized: ${successCount}  Failed: ${errorCount}  Reason: ${error}`
          response.failedJobSetReprioritizations.push({ jobSet: jobSet, error: message })
        }
      } catch (e) {
        console.error(e)
        const text = await getErrorMessage(e)
        response.failedJobSetReprioritizations.push({ jobSet: jobSet, error: text })
      }
    }
    return response
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
    const jobStateDuration = jobInfo.jobStateDuration ?? "-"
    const jobFromJson = jobInfo.jobJson ? (JSON.parse(jobInfo.jobJson) as ApiJob) : undefined
    const jobYaml = jobInfo.jobJson ? jobJsonToYaml(jobFromJson) : ""
    const runs = getRuns(jobInfo)
    const annotations = jobInfo.job?.annotations ? this.getAnnotations(jobInfo.job?.annotations) : {}
    const namespace = jobFromJson?.namespace ?? ""
    const containers = getContainers(jobFromJson)

    return {
      jobId: jobId,
      queue: queue,
      owner: owner,
      jobSet: jobSet,
      priority: priority,
      submissionTime: submissionTime,
      cancelledTime: cancelledTime,
      jobState: jobState,
      jobStateDuration: jobStateDuration,
      runs: runs,
      jobYaml: jobYaml,
      annotations: annotations,
      namespace: namespace,
      containers: containers,
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

function jobSetToViewModel(jobSet: LookoutJobSetInfo): JobSet {
  return {
    jobSetId: jobSet.jobSet ?? "Unknown job set",
    queue: jobSet.queue ?? "Unknown queue",
    jobsQueued: jobSet.jobsQueued ?? 0,
    jobsPending: jobSet.jobsPending ?? 0,
    jobsRunning: jobSet.jobsRunning ?? 0,
    jobsSucceeded: jobSet.jobsSucceeded ?? 0,
    jobsFailed: jobSet.jobsFailed ?? 0,
    jobsCancelled: jobSet.jobsCancelled ?? 0,
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

function getContainers(apiJob: ApiJob | undefined): Map<number, string[]> {
  const podSpecs = apiJob?.podSpecs || []
  if (podSpecs.length === 0 && apiJob?.podSpec) {
    podSpecs.push(apiJob?.podSpec)
  }
  if (podSpecs.length === 0) {
    return new Map<number, string[]>()
  }
  return getContainersFromPodSpecs(podSpecs)
}

function getContainersFromPodSpecs(podSpecs: V1PodSpec[]): Map<number, string[]> {
  const containers = new Map<number, string[]>()
  for (let i = 0; i < podSpecs.length; i++) {
    const podContainerNames: string[] = []

    const initContainers = getContainerNames(podSpecs[i].initContainers)
    const podContainers = getContainerNames(podSpecs[i].containers)
    podContainerNames.push(...initContainers, ...podContainers)

    if (podContainerNames.length === 0) {
      podContainerNames.push(UNKNOWN_CONTAINER)
    }

    containers.set(i, podContainerNames)
  }
  return containers
}

function getContainerNames(containers: V1Container[] | undefined): string[] {
  return (containers ?? []).map((container) => container.name ?? UNKNOWN_CONTAINER)
}

function getRuns(jobInfo: LookoutJobInfo): Run[] {
  if (!jobInfo.runs || jobInfo.runs.length === 0) {
    return []
  }

  sortRuns(jobInfo.runs)
  return jobInfo.runs.map((r) => runInfoToViewModel(r))
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

function runInfoToViewModel(run: LookoutRunInfo): Run {
  return {
    k8sId: run.k8sId ?? "Unknown Kubernetes id",
    cluster: run.cluster ?? "Unknown cluster",
    node: run.node,
    succeeded: run.succeeded ?? false,
    error: run.error,
    podCreationTime: run.created ? dateToString(run.created) : undefined,
    podStartTime: run.started ? dateToString(run.started) : undefined,
    finishTime: run.finished ? dateToString(run.finished) : undefined,
    preemptedTime: run.preempted ? dateToString(run.preempted) : undefined,
    podNumber: run.podNumber ?? 0,
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
