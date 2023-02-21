import { Job, JobFilter, JobGroup, JobKey, JobOrder } from "models/lookoutV2Models"
import { GroupJobsResponse, IGroupJobsService } from "services/lookoutV2/GroupJobsService"
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
    const groups = groupBy(filtered, groupedField, aggregates)
    const sliced = groups.sort(comparator(order)).slice(skip, skip + take)
    return {
      groups: sliced,
      count: groups.length,
    }
  }
}

type AggregateType = "Max" | "Average"

type AggregateField = {
  field: JobKey
  aggregateType: AggregateType
}

const aggregateFieldMap = new Map<string, AggregateField>([
  ["submitted", { field: "submitted", aggregateType: "Max" }],
  ["lastTransitionTime", { field: "lastTransitionTime", aggregateType: "Average" }],
])

function groupBy(jobs: Job[], field: string, aggregates: string[]): JobGroup[] {
  const groups = new Map<string | number, Job[]>()
  for (const job of jobs) {
    let value = job[field as JobKey]
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
    const computedAggregates: Record<string, string> = {}
    for (const aggregate of aggregates) {
      if (!aggregateFieldMap.has(aggregate)) {
        continue
      }
      const aggregateField = aggregateFieldMap.get(aggregate) as AggregateField
      const values = jobs.map((job) => new Date(job[aggregateField.field] as string).getTime())
      switch (aggregateField.aggregateType) {
        case "Max":
          const max = Math.max(...values)
          computedAggregates[aggregateField.field] = new Date(max).toISOString()
          break
        case "Average":
          const avg = values.reduce((a, b) => a + b, 0) / values.length
          computedAggregates[aggregateField.field] = new Date(avg).toISOString()
          break
        default:
          console.error(`aggregate type not found: ${aggregateField.aggregateType}`)
          break
      }
    }
    return {
      name: groupName,
      count: jobs.length,
      aggregates: computedAggregates,
    } as JobGroup
  })
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
