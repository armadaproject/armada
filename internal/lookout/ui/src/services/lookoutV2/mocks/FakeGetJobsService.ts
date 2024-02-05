import { Job, JobFilter, JobKey, JobOrder } from "models/lookoutV2Models"
import { GetJobsResponse, IGetJobsService } from "services/lookoutV2/GetJobsService"
import { compareValues, getActiveJobSets, mergeFilters, simulateApiWait } from "utils/fakeJobsUtils"

export default class FakeGetJobsService implements IGetJobsService {
  constructor(private jobs: Job[], private simulateApiWait = true) {}

  async getJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    skip: number,
    take: number,
    signal?: AbortSignal,
  ): Promise<GetJobsResponse> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }

    let filtered = this.jobs.filter(mergeFilters(filters)).sort(comparator(order))
    if (activeJobSets) {
      const active = getActiveJobSets(filtered)
      filtered = filtered.filter((job) => job.queue in active && active[job.queue].includes(job.jobSet))
    }
    return {
      jobs: filtered.slice(skip, skip + take),
    }
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
