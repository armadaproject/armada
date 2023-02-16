import { Job, JobFilter, JobGroup, JobKey, JobOrder } from "models/lookoutV2Models"
import { IGroupJobsService, GroupJobsResponse } from "services/lookoutV2/GroupJobsService"
import { compareValues, mergeFilters, simulateApiWait } from "utils/fakeJobsUtils"

export default class FakeGroupJobsService implements IGroupJobsService {
  constructor(private jobs: Job[], private simulateApiWait = true) {}

  async groupJobs(
    filters: JobFilter[],
    order: JobOrder,
    groupedField: string,
    aggregates: string[],
    skip: number,
    take: number,
    signal: AbortSignal | undefined,
  ): Promise<GroupJobsResponse> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }

    const filtered = this.jobs.filter(mergeFilters(filters))
    const groups = groupBy(filtered, groupedField)
    const sliced = groups.sort(comparator(order)).slice(skip, skip + take)
    const response: GroupJobsResponse = {
      groups: sliced,
      count: groups.length,
    }
    return response
  }
}

function groupBy(jobs: Job[], field: string): JobGroup[] {
  const groups = new Map<string | number, JobGroup>()
  for (const job of jobs) {
    let value = job[field as JobKey]
    if (value === undefined) {
      continue
    }
    value = value as string | number
    if (groups.has(value)) {
      const group = groups.get(value)
      if (group !== undefined) {
        group.count += 1
        groups.set(value, group)
      }
    } else {
      groups.set(value, {
        name: value as string,
        count: 1,
        aggregates: {},
      })
    }
  }
  return Array.from(groups.values())
}

function comparator(order: JobOrder): (a: JobGroup, b: JobGroup) => number {
  return (a, b) => {
    let accessor: (group: JobGroup) => string | number | undefined = () => undefined
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
      console.error(`group accessor for field ${order.field} is undefined`, { a, b })
      return 0
    }
    return compareValues(valueA, valueB, order.direction)
  }
}
