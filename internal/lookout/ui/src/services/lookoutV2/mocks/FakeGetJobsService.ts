import { Job, JobFilter, JobKey, JobOrder } from "models/lookoutV2Models"
import GetJobsService, { GetJobsResponse } from "services/lookoutV2/GetJobsService"
import { compareValues, mergeFilters, simulateApiWait } from "utils/fakeJobsUtils"

export default class FakeGetJobsService implements GetJobsService {
  jobs: Job[]

  constructor(jobs: Job[]) {
    this.jobs = jobs
  }

  async getJobs(
    filters: JobFilter[],
    order: JobOrder,
    skip: number,
    take: number,
    signal: AbortSignal | undefined,
  ): Promise<GetJobsResponse> {
    console.log("Making GetJobs call with params:", { filters, order, skip, take, signal })
    await simulateApiWait()
    const filtered = this.jobs.filter(mergeFilters(filters)).sort(comparator(order))
    const response = {
      totalJobs: filtered.length,
      jobs: filtered.slice(skip, skip + take),
    }
    console.log("GetJobs response", response)
    return response
  }
}

function comparator(order: JobOrder): (a: Job, b: Job) => number {
  return (a, b) => {
    const field = order.field as JobKey
    const valueA = a[field]
    const valueB = b[field]

    if (valueA === undefined || valueB === undefined) {
      console.error("comparator values are undefined")
      return 0
    }

    return compareValues(valueA, valueB, order.direction)
  }
}
