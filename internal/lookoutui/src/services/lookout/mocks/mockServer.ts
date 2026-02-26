import { http, HttpResponse, PathParams } from "msw"
import { setupServer, SetupServerApi } from "msw/node"

import { compareValues, getActiveJobSets, mergeFilters } from "../../../common/fakeJobsUtils"
import { Job, JobFilter, JobId, JobKey, JobOrder } from "../../../models/lookoutModels"
import { CancelJobsRequest } from "../../../openapi/armada"
import { FAKE_ARMADA_API_BASE_URL } from "../../../setupTests"

const GET_QUEUES_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/batched/queues`
const POST_JOB_RUN_ERROR_ENDPOINT = "/api/v1/jobRunError"
const POST_JOB_RUN_DEBUG_MESSAGE_ENDPOINT = "/api/v1/jobRunDebugMessage"
const POST_JOBS_ENDPOINT = "/api/v1/jobs"
const CANCEL_JOBS_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/job/cancel`
const REPRIORITIZE_JOBS_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/job/reprioritize`
const PREEMPT_JOBS_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/job/preempt`

export class MockServer {
  private server: SetupServerApi

  constructor() {
    this.server = setupServer()
  }

  listen() {
    return this.server.listen({ onUnhandledRequest: "error" })
  }

  reset() {
    return this.server.restoreHandlers()
  }

  close() {
    return this.server.close()
  }

  setGetQueuesResponse(queueNames: string[]) {
    this.server.use(
      http.get(GET_QUEUES_ENDPOINT, () =>
        HttpResponse.text(
          queueNames
            .map((name) =>
              JSON.stringify({
                result: { Event: { queue: { name } } },
              }),
            )
            .join("\n"),
        ),
      ),
    )
  }

  setPostJobRunErrorResponseForRunId(runId: string, errorMessage: string) {
    this.server.use(
      http.post<object, { runId: string }, { errorMessage: string }>(POST_JOB_RUN_ERROR_ENDPOINT, async (req) => {
        const reqJson = await req.request.json()
        if (reqJson.runId === runId) {
          return HttpResponse.json({ errorMessage })
        }
      }),
    )
  }

  setPostJobRunDebugMessageResponseForRunId(runId: string, errorMessage: string) {
    this.server.use(
      http.post<object, { runId: string }, { errorMessage: string }>(
        POST_JOB_RUN_DEBUG_MESSAGE_ENDPOINT,
        async (req) => {
          const reqJson = await req.request.json()
          if (reqJson.runId === runId) {
            return HttpResponse.json({ errorMessage })
          }
        },
      ),
    )
  }

  setPostJobsResponse(jobs: Job[]) {
    this.server.use(
      http.post<
        object,
        { skip?: number; take?: number; filters?: JobFilter[]; activeJobSets?: boolean; order?: JobOrder }
      >(POST_JOBS_ENDPOINT, async (req) => {
        const reqJson = await req.request.json()
        const skip = reqJson.skip ?? 0
        const take = reqJson.take ?? jobs.length
        const filters = reqJson.filters ?? []
        const activeJobSets = reqJson.activeJobSets ?? false
        const order = reqJson.order

        // Apply filters
        let filtered = jobs.filter(mergeFilters(filters))

        // Apply active job sets filter
        if (activeJobSets) {
          const active = getActiveJobSets(filtered)
          filtered = filtered.filter((job) => job.queue in active && active[job.queue].includes(job.jobSet))
        }

        // Apply sorting
        if (order) {
          filtered = filtered.sort((a, b) => {
            const field = order.field as JobKey
            const valueA = a[field]
            const valueB = b[field]
            if (valueA === undefined || valueB === undefined) {
              return 0
            }
            return compareValues(valueA, valueB, order.direction)
          })
        }

        // Apply pagination
        const paginatedJobs = filtered.slice(skip, skip + take)
        return HttpResponse.json({ jobs: paginatedJobs })
      }),
    )
  }

  setCancelJobsResponse(successfulJobIds: JobId[], failedJobIds: JobId[] = []) {
    this.server.use(
      http.post<PathParams, CancelJobsRequest["body"]>(CANCEL_JOBS_ENDPOINT, async (req) => {
        const reqJson = await req.request.json()
        const requestedJobIds = reqJson.jobIds as JobId[]

        const cancelledIds = requestedJobIds.filter(
          (jobId) => successfulJobIds.includes(jobId) && !failedJobIds.some((f) => f === jobId),
        )

        return HttpResponse.json({
          cancelledIds,
        })
      }),
    )
  }

  setReprioritizeJobsResponse(successfulJobIds: JobId[], failedJobIds: { jobId: JobId; errorReason: string }[] = []) {
    this.server.use(
      http.post(REPRIORITIZE_JOBS_ENDPOINT, async () => {
        const reprioritizationResults: Record<JobId, string> = {}
        for (const jobId of successfulJobIds) {
          reprioritizationResults[jobId] = ""
        }
        for (const { jobId, errorReason } of failedJobIds) {
          reprioritizationResults[jobId] = errorReason
        }
        return HttpResponse.json({
          reprioritizationResults,
        })
      }),
    )
  }

  setPreemptJobsResponse(successfulJobIds: JobId[], failedJobIds: { jobId: JobId; errorReason: string }[] = []) {
    this.server.use(
      http.post(PREEMPT_JOBS_ENDPOINT, async () => {
        const preemptionResults: Record<JobId, string> = {}
        for (const jobId of successfulJobIds) {
          preemptionResults[jobId] = ""
        }
        for (const { jobId, errorReason } of failedJobIds) {
          preemptionResults[jobId] = errorReason
        }
        return HttpResponse.json({
          preemptionResults,
        })
      }),
    )
  }
}
