import { JobFilters, makeQueryStringFromFilters } from "./JobsContainer";
import { JobStateViewModel } from "../services/JobService";

function assertStringHasQueryParams(expected: string[], actual: string) {
  const actualQueryParams = actual.split("&")
  expect(expected.sort()).toStrictEqual(actualQueryParams.sort())
}

describe("makeQueryStringFromFilters", () => {
  test("makes string with queue", () => {
    const filters: JobFilters = {
      queue: "test",
      jobSet: "",
      jobStates: [],
      newestFirst: false,
    }
    const queryString = makeQueryStringFromFilters(filters)
    assertStringHasQueryParams(["queue=test", "newest_first=false"], queryString)
  })

  test("makes string with job set", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "job-set",
      jobStates: [],
      newestFirst: false,
    }
    const queryString = makeQueryStringFromFilters(filters)
    assertStringHasQueryParams(["job_set=job-set", "newest_first=false"], queryString)
  })

  test("makes string with single job state", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "",
      jobStates: [JobStateViewModel.Queued],
      newestFirst: false,
    }
    const queryString = makeQueryStringFromFilters(filters)
    assertStringHasQueryParams(["job_states=Queued", "newest_first=false"], queryString)
  })

  test("makes string with multiple job states", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "",
      jobStates: [JobStateViewModel.Queued, JobStateViewModel.Running, JobStateViewModel.Cancelled],
      newestFirst: false,
    }
    const queryString = makeQueryStringFromFilters(filters)
    assertStringHasQueryParams(["job_states=Queued,Running,Cancelled", "newest_first=false"], queryString)
  })

  test("makes string with ordering", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    }
    const queryString = makeQueryStringFromFilters(filters)
    assertStringHasQueryParams(["newest_first=true"], queryString)
  })

  test("makes string with all filters", () => {
    const filters: JobFilters = {
      queue: "other-test",
      jobSet: "other-job-set",
      jobStates: [JobStateViewModel.Pending, JobStateViewModel.Succeeded, JobStateViewModel.Failed],
      newestFirst: true,
    }
    const queryString = makeQueryStringFromFilters(filters)
    assertStringHasQueryParams([
      "queue=other-test",
      "job_set=other-job-set",
      "job_states=Pending,Succeeded,Failed",
      "newest_first=true",
    ], queryString)
  })
})

describe("makeFiltersFromQueryString", () => {
  test("make filters with queue", () => {
    const queryString = "queue=test"

  })
})

