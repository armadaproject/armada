import { http, HttpResponse, PathParams } from "msw"
import { setupServer, SetupServerApi } from "msw/node"

import { compareValues, getActiveJobSets, mergeFilters } from "../../../common/fakeJobsUtils"
import { Job, JobFilter, JobGroup, JobId, JobKey, JobOrder, SortDirection } from "../../../models/lookoutModels"
import { CancelJobsRequest } from "../../../openapi/armada"
import { FAKE_ARMADA_API_BASE_URL } from "../../../setupTests"

const GET_QUEUES_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/batched/queues`
const POST_JOB_RUN_ERROR_ENDPOINT = "/api/v1/jobRunError"
const POST_JOB_RUN_DEBUG_MESSAGE_ENDPOINT = "/api/v1/jobRunDebugMessage"
const POST_JOBS_ENDPOINT = "/api/v1/jobs"
const POST_JOB_GROUPS_ENDPOINT = "/api/v1/jobGroups"
const POST_JOB_SPEC_ENDPOINT = "/api/v1/jobSpec"
const CANCEL_JOBS_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/job/cancel`
const REPRIORITIZE_JOBS_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/job/reprioritize`
const PREEMPT_JOBS_ENDPOINT = `${FAKE_ARMADA_API_BASE_URL}/v1/job/preempt`

interface GroupedField {
  field: string
  isAnnotation: boolean
}

type AggregateType = "Max" | "Average" | "State Counts"

type AggregateField = {
  field: JobKey
  aggregateType: AggregateType
}

const aggregateFieldMap = new Map<string, AggregateField>([
  ["submitted", { field: "submitted", aggregateType: "Max" }],
  ["lastTransitionTime", { field: "lastTransitionTime", aggregateType: "Average" }],
  ["state", { field: "state", aggregateType: "State Counts" }],
])

function groupBy(jobs: Job[], groupedField: GroupedField, aggregates: string[]): JobGroup[] {
  const groups = new Map<string | number, Job[]>()
  for (const job of jobs) {
    let value = job[groupedField.field as JobKey]
    if (groupedField.isAnnotation) {
      value = job.annotations[groupedField.field]
    }
    if (value === undefined) {
      continue
    }
    value = value as string | number
    if (groups.has(value)) {
      groups.get(value)?.push(job)
    } else {
      groups.set(value, [job])
    }
  }
  return Array.from(groups.entries()).map(([groupName, jobs]) => {
    const computedAggregates: Record<string, string | Record<string, number>> = {}
    for (const aggregate of aggregates) {
      if (!aggregateFieldMap.has(aggregate)) {
        continue
      }
      const aggregateField = aggregateFieldMap.get(aggregate) as AggregateField
      switch (aggregateField.aggregateType) {
        case "Max": {
          const max = Math.max(...jobs.map((job) => new Date(job[aggregateField.field] as string).getTime()))
          computedAggregates[aggregateField.field] = new Date(max).toISOString()
          break
        }
        case "Average": {
          const values = jobs.map((job) => new Date(job[aggregateField.field] as string).getTime())
          const avg = values.reduce((a, b) => a + b, 0) / values.length
          computedAggregates[aggregateField.field] = new Date(avg).toISOString()
          break
        }
        case "State Counts": {
          const stateCounts: Record<string, number> = {}
          for (const job of jobs) {
            if (!(job.state in stateCounts)) {
              stateCounts[job.state] = 0
            }
            stateCounts[job.state] += 1
          }
          computedAggregates[aggregateField.field] = stateCounts
          break
        }
      }
    }
    return {
      name: groupName,
      count: jobs.length,
      aggregates: computedAggregates,
    } as JobGroup
  })
}

function groupComparator(order: JobOrder): (a: JobGroup, b: JobGroup) => number {
  return (a, b) => {
    let accessor: (group: JobGroup) => string | number | Record<string, number> | undefined = () => undefined
    if (order.field === "count") {
      accessor = (group: JobGroup) => group.count
    } else if (order.field === "name") {
      accessor = (group: JobGroup) => group.name
    } else {
      accessor = (group: JobGroup) => group.aggregates[order.field]
    }

    const valueA = accessor(a)
    const valueB = accessor(b)
    if (valueA === undefined || valueB === undefined) {
      return 0
    }
    return compareValues(valueA, valueB, order.direction as SortDirection)
  }
}

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

    this.server.use(
      http.post<
        object,
        {
          filters?: JobFilter[]
          activeJobSets?: boolean
          order?: JobOrder
          groupedField?: GroupedField
          aggregates?: string[]
          skip?: number
          take?: number
        }
      >(POST_JOB_GROUPS_ENDPOINT, async (req) => {
        const reqJson = await req.request.json()
        const filters = reqJson.filters ?? []
        const activeJobSets = reqJson.activeJobSets ?? false
        const order = reqJson.order ?? { field: "count", direction: "DESC" }
        const groupedField = reqJson.groupedField ?? { field: "queue", isAnnotation: false }
        const aggregates = reqJson.aggregates ?? []
        const skip = reqJson.skip ?? 0
        const take = reqJson.take ?? 0

        let filtered = jobs.filter(mergeFilters(filters))

        if (activeJobSets) {
          const active = getActiveJobSets(filtered)
          filtered = filtered.filter((job) => job.queue in active && active[job.queue].includes(job.jobSet))
        }

        const groups = groupBy(filtered, groupedField, aggregates)
        const sorted = groups.sort(groupComparator(order))
        const sliced = take === 0 ? sorted.slice(skip) : sorted.slice(skip, skip + take)

        return HttpResponse.json({ groups: sliced })
      }),
    )
  }

  setPostJobSpecResponse(jobSpec: Record<string, unknown>) {
    this.server.use(
      http.post<object, { jobId: string }>(POST_JOB_SPEC_ENDPOINT, async (req) => {
        const reqJson = await req.request.json()
        return HttpResponse.json({ job: { ...jobSpec, id: reqJson.jobId } })
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
