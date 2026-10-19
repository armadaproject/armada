import { JobFilter, JobRunState, JobState, Match } from "../models/lookoutModels"
import { getFakeJobsResponse } from "../services/lookout/useGetJobs"

import { makeRandomJobs, makeTestJob, mergeFilters } from "./fakeJobsUtils"
import { getLatestRunId } from "./jobsTableColumns"

const makeRun = (jobId: string, runId: string) => ({
  runId,
  jobId,
  cluster: "cluster",
  jobRunState: JobRunState.RunSucceeded,
  leased: "",
  pending: "",
  started: "",
  finished: "",
})

describe("run ID filtering", () => {
  const multiRunJob = makeTestJob("q", "js", "job-1", JobState.Running, undefined, [
    makeRun("job-1", "run-a"),
    makeRun("job-1", "run-b"),
  ])
  const singleRunJob = makeTestJob("q", "js", "job-2", JobState.Running, undefined, [makeRun("job-2", "run-c")])
  const noRunJob = makeTestJob("q", "js", "job-3", JobState.Queued)
  const jobs = [multiRunJob, singleRunJob, noRunJob]

  const filter = (...filters: JobFilter[]) => jobs.filter(mergeFilters(filters)).map((j) => j.jobId)

  it("matches any run of a job, not just the latest", () => {
    expect(filter({ field: "runId", match: Match.Exact, value: "run-a" })).toEqual(["job-1"])
    expect(filter({ field: "runId", match: Match.Exact, value: "run-b" })).toEqual(["job-1"])
    expect(filter({ field: "runId", match: Match.Exact, value: "run-c" })).toEqual(["job-2"])
  })

  it("matches nothing for an unknown run ID", () => {
    expect(filter({ field: "runId", match: Match.Exact, value: "nope" })).toEqual([])
  })

  it("supports matching any of several run IDs", () => {
    expect(filter({ field: "runId", match: Match.AnyOf, value: ["run-a", "run-c"] })).toEqual(["job-1", "job-2"])
  })
})

describe("getLatestRunId", () => {
  it("returns the last run's ID, or an empty string if there are no runs", () => {
    const job = makeTestJob("q", "js", "job-1", JobState.Running, undefined, [
      makeRun("job-1", "run-a"),
      makeRun("job-1", "run-b"),
    ])
    expect(getLatestRunId({ ...job, rowId: "job-1" } as never)).toBe("run-b")
    expect(getLatestRunId({ ...makeTestJob("q", "js", "job-2", JobState.Queued), rowId: "job-2" } as never)).toBe("")
  })
})

describe("makeRandomJobs", () => {
  const ULID_PATTERN = /^[0-7][0-9a-hjkmnp-tv-z]{25}$/

  it("generates ULID job IDs, unique across jobs", () => {
    const jobIds = makeRandomJobs(50, 42).map((j) => j.jobId)
    expect(new Set(jobIds).size).toBe(50)
    for (const jobId of jobIds) {
      expect(jobId).toMatch(ULID_PATTERN)
    }
  })

  it("generates ULID run IDs", () => {
    const runIds = makeRandomJobs(50, 42).flatMap((j) => j.runs.map((r) => r.runId))
    expect(runIds.length).toBeGreaterThan(0)
    for (const runId of runIds) {
      expect(runId).toMatch(ULID_PATTERN)
    }
  })

  it("generates the same IDs for the same seed", () => {
    const ids = (seed: number) => makeRandomJobs(20, seed).map((j) => [j.jobId, ...j.runs.map((r) => r.runId)])
    expect(ids(42)).toEqual(ids(42))
  })

  it("generates some jobs with multiple runs", () => {
    const jobs = makeRandomJobs(100, 42)
    expect(jobs.filter((j) => j.runs.length > 1).length).toBeGreaterThan(10)
  })
})

describe("sorting fake jobs by run ID", () => {
  const latestRunIds = (direction: "ASC" | "DESC") =>
    getFakeJobsResponse({
      filters: [],
      activeJobSets: false,
      order: { field: "runId", direction },
      skip: 0,
      take: 10_000,
    }).jobs.map((j) => j.lastActiveRunId)

  it("sorts by the latest run ID, with jobs without runs last when ascending", () => {
    const ids = latestRunIds("ASC")
    const withRuns = ids.filter((id): id is string => id !== undefined)
    expect(withRuns.length).toBeGreaterThan(0)
    expect(withRuns).toEqual([...withRuns].sort())
    // Jobs without runs sort as the largest value, so appear after those with runs
    expect(ids.slice(0, withRuns.length)).toEqual(withRuns)
  })

  it("reverses the order when descending", () => {
    expect(latestRunIds("DESC")).toEqual([...latestRunIds("ASC")].reverse())
  })
})
