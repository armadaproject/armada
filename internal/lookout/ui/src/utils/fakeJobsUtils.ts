import { isString } from "lodash"
import { Job, JobFilter, JobKey, JobRun, JobRunState, JobState, Match, SortDirection } from "models/lookoutV2Models"
import { v4 as uuidv4 } from "uuid"

export function randomInt(min: number, max: number, rand: () => number) {
  const range = max - min
  return min + Math.floor(rand() * range)
}

export function mulberry32(a: number): () => number {
  return () => {
    let t = (a += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export function seededUuid(rand: () => number): () => string {
  return () =>
    uuidv4({
      rng: () => {
        const floatArray = new Float32Array(4)
        floatArray[0] = rand()
        floatArray[1] = rand()
        floatArray[2] = rand()
        floatArray[3] = rand()
        const buf = new Uint8Array(floatArray.buffer)
        return Array.from(buf)
      },
    })
}

export async function simulateApiWait(abortSignal?: AbortSignal): Promise<void> {
  await new Promise((resolve, reject) => {
    const timeoutId = setTimeout(resolve, randomInt(200, 1000, Math.random))
    abortSignal?.addEventListener("abort", () => {
      clearTimeout(timeoutId)
      reject()
    })
  })
}

export function makeTestJobs(nJobs: number, seed: number, nQueues = 10, nJobSets = 100): Job[] {
  const rand = mulberry32(seed)
  const uuid = seededUuid(rand)
  const annotationKeys = ["hyperparameter", "some/very/long/annotation/key/name/with/forward/slashes", "region"]

  const queues = Array.from(Array(nQueues).keys()).map((i) => `queue-${i + 1}`)
  const jobSets = Array.from(Array(nJobSets).keys()).map((i) => `job-set-${i + 1}`)

  const jobs: Job[] = []
  for (let i = 0; i < nJobs; i++) {
    const jobId = "01gkv9cj53h0rk9407mds" + i
    const runs = createJobRuns(randomInt(0, 3, rand), jobId, rand, uuid)

    jobs.push({
      gpu: randomInt(0, 8, rand),
      lastActiveRunId: runs.length > 0 ? runs[runs.length - 1].runId : undefined,
      owner: uuid(),
      priority: randomInt(0, 1000, rand),
      runs: runs,
      submitted: "2022-12-13T11:57:25.733Z",
      cpu: randomInt(2, 200, rand) * 100,
      ephemeralStorage: 34359738368,
      memory: 134217728,
      queue: queues[i % queues.length],
      annotations: createAnnotations(annotationKeys, uuid),
      jobId: jobId,
      jobSet: jobSets[i % jobSets.length],
      state: randomProperty(JobState, rand),
      lastTransitionTime: "2022-12-13T12:19:14.956Z",
    })
  }

  return jobs
}

function createJobRuns(n: number, jobId: string, rand: () => number, uuid: () => string): JobRun[] {
  if (n === 0) {
    return []
  }

  const runs: JobRun[] = []
  for (let i = 0; i < n; i++) {
    runs.push({
      cluster: uuid(),
      error: "something bad might have happened?",
      exitCode: randomInt(0, 64, rand),
      finished: "2022-12-13T12:19:14.956Z",
      jobId: jobId,
      jobRunState: randomProperty(JobRunState, rand),
      node: uuid(),
      pending: "2022-12-13T12:16:14.956Z",
      runId: uuid(),
      started: "2022-12-13T12:15:14.956Z",
    })
  }
  return runs
}

function randomProperty<T>(obj: Record<string, T>, rand: () => number): T {
  const keys = Object.keys(obj)
  return obj[keys[(keys.length * rand()) << 0]]
}

function createAnnotations(annotationKeys: string[], uuid: () => string): Record<string, string> {
  const annotations: Record<string, string> = {}
  for (const key of annotationKeys) {
    annotations[key] = uuid()
  }
  return annotations
}

export function mergeFilters(filters: JobFilter[]): (job: Job) => boolean {
  return filters.map(filterFn).reduce(
    (aggregatedFn, newFn) => (job) => aggregatedFn(job) && newFn(job),
    () => true,
  )
}

export function filterFn(filter: JobFilter): (job: Job) => boolean {
  return (job) => {
    const objectToFilter = filter.isAnnotation ? job.annotations : job

    if (!Object.prototype.hasOwnProperty.call(objectToFilter, filter.field)) {
      console.error(`Unknown filter field provided: ${filter}`)
      return false
    }
    const matcher = getMatch(filter.match)
    return matcher(objectToFilter[filter.field as JobKey], filter.value)
  }
}

export function getMatch(match: Match): (a: any, b: any) => boolean {
  switch (match) {
    case "exact":
      return (a, b) => a === b
    case "startsWith":
      return (a, b) => isString(a) && isString(b) && a.startsWith(b)
    case "greater":
      return (a, b) => a > b
    case "less":
      return (a, b) => a < b
    case "greaterOrEqual":
      return (a, b) => a >= b
    case "lessOrEqual":
      return (a, b) => a <= b
    case "anyOf":
      return (a, b) => b.includes(a)
    default:
      console.error(`Unknown match: ${match}`)
      return () => false
  }
}

export function compareValues(valueA: any, valueB: any, direction: SortDirection): number {
  let val = 0
  if (valueA < valueB) {
    val = -1
  } else if (valueA > valueB) {
    val = 1
  } else if (valueA === valueB) {
    val = 0
  }
  if (direction === "DESC") {
    val = -val
  }
  return val
}
