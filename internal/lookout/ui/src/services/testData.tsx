import { CancelJobSetsResult, DurationStats, Job, JobSet, QueueInfo } from "./JobService"

function generateId(length: number): string {
  const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  const charactersLength = characters.length
  const arr = new Array(length)
  for (let i = 0; i < length; i++) {
    arr[i] = characters.charAt(Math.floor(Math.random() * charactersLength))
  }
  return arr.join("")
}

// Both inclusive
function randInt(low: number, high: number): number {
  low = Math.ceil(low)
  high = Math.floor(high)
  return Math.floor(Math.random() * (high - low + 1)) + low
}

function makeDurationStats(): DurationStats {
  return {
    shortest: randInt(1, 5),
    q1: randInt(5, 10),
    median: randInt(10, 15),
    average: randInt(10, 15),
    q3: randInt(15, 20),
    longest: randInt(20, 25),
  }
}

export function makeTestJobs(queue: string, start: number, stop: number): Job[] {
  const jobs: Job[] = []

  for (let i = start; i < stop; i++) {
    jobs.push({
      annotations: {},
      jobId: `${i}`,
      jobSet: generateId(10),
      jobState: "Queued",
      jobYaml: "",
      owner: generateId(5),
      priority: 1,
      queue: queue,
      runs: [
        {
          k8sId: "k8s id",
          cluster: "cluster",
          node: "node",
          succeeded: false,
          error: "Something bad happened",
          podCreationTime: "a time",
          podStartTime: "another time",
          finishTime: "yet another time",
          podNumber: 0,
          containers: [],
        },
      ],
      submissionTime: "some time",
      namespace: "namespace",
    })
  }

  return jobs
}

export function makeTestJobSets(nJobSets: number, jobSetLength: number): JobSet[] {
  const jobSets: JobSet[] = []

  for (let i = 0; i < nJobSets; i++) {
    jobSets.push({
      jobSetId: generateId(jobSetLength),
      queue: "test",
      jobsQueued: randInt(1, 50),
      jobsPending: randInt(1, 50),
      jobsRunning: randInt(1, 50),
      jobsSucceeded: randInt(1, 50),
      jobsFailed: randInt(1, 50),
      runningStats: makeDurationStats(),
      queuedStats: makeDurationStats(),
      latestSubmissionTime: "some time",
    })
  }

  return jobSets
}

export function makeTestOverview(nQueues: number, queueLength: number): QueueInfo[] {
  const queueInfos: QueueInfo[] = []

  for (let i = 0; i < nQueues; i++) {
    queueInfos.push({
      jobsPending: 0,
      jobsQueued: 0,
      jobsRunning: 0,
      longestRunningDuration: "",
      oldestQueuedDuration: "",
      queue: generateId(queueLength),
    })
  }

  return queueInfos
}

export function makeTestCancelJobSetsResults(nJobSets: number, jobSetLength: number): CancelJobSetsResult {
  const results: CancelJobSetsResult = {
    cancelledJobSets: [],
    failedJobSetCancellations: [],
  }

  for (let i = 0; i < nJobSets; i++) {
    results.cancelledJobSets.push({
      jobSetId: generateId(jobSetLength),
      queue: "test",
      jobsQueued: randInt(1, 50),
      jobsPending: randInt(1, 50),
      jobsRunning: randInt(1, 50),
      jobsSucceeded: randInt(1, 50),
      jobsFailed: randInt(1, 50),
      runningStats: makeDurationStats(),
      queuedStats: makeDurationStats(),
      latestSubmissionTime: "some time",
    })
    results.failedJobSetCancellations.push({
      jobSet: {
        jobSetId: generateId(jobSetLength),
        queue: "test",
        jobsQueued: randInt(1, 50),
        jobsPending: randInt(1, 50),
        jobsRunning: randInt(1, 50),
        jobsSucceeded: randInt(1, 50),
        jobsFailed: randInt(1, 50),
        runningStats: makeDurationStats(),
        queuedStats: makeDurationStats(),
        latestSubmissionTime: "some time",
      },
      error: "Some bad error happened ..........................................................................",
    })
  }

  return results
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
