import { Job, JobFilter, JobGroup, JobKey, JobOrder } from "models/lookoutV2Models"
import { GroupedField, GroupJobsResponse, IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { compareValues, getActiveJobSets, mergeFilters, simulateApiWait } from "utils/fakeJobsUtils"

export default class FakeGroupJobsService implements IGroupJobsService {
  constructor(private jobs: Job[], private simulateApiWait = true) {}

  async groupJobs(
    filters: JobFilter[],
    activeJobSets: boolean,
    order: JobOrder,
    groupedField: GroupedField,
    aggregates: string[],
    skip: number,
    take: number,
    signal: AbortSignal | undefined,
  ): Promise<GroupJobsResponse> {
    if (this.simulateApiWait) {
      await simulateApiWait(signal)
    }

    let filtered = this.jobs.filter(mergeFilters(filters))
    if (activeJobSets) {
      const active = getActiveJobSets(filtered)
      filtered = filtered.filter((job) => job.queue in active && active[job.queue].includes(job.jobSet))
    }
    const groups = groupBy(filtered, groupedField, aggregates)
    const sorted = groups.sort(comparator(order))
    // This matches the behavior of version 2 of the Lookout API, which
    // understands zero to mean "no pagination" (in order to support queries
    // without pagination, such as those used by `/job-sets`).
    const sliced = take == 0 ? sorted.slice(skip) : sorted.slice(skip, skip + take)
    return {
      groups: sliced,
      count: groups.length,
    }
  }
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
        case "Max":
          const max = Math.max(...jobs.map((job) => new Date(job[aggregateField.field] as string).getTime()))
          computedAggregates[aggregateField.field] = new Date(max).toISOString()
          break
        case "Average":
          const values = jobs.map((job) => new Date(job[aggregateField.field] as string).getTime())
          const avg = values.reduce((a, b) => a + b, 0) / values.length
          computedAggregates[aggregateField.field] = new Date(avg).toISOString()
          break
        case "State Counts":
          const stateCounts: Record<string, number> = {}
          for (const job of jobs) {
            if (!(job.state in stateCounts)) {
              stateCounts[job.state] = 0
            }
            stateCounts[job.state] += 1
          }
          computedAggregates[aggregateField.field] = stateCounts
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
      console.error(`group accessor for field ${order.field} is undefined`, { a, b })
      return 0
    }
    return compareValues(valueA, valueB, order.direction)
  }
}
