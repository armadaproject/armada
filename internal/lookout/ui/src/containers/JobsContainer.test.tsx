import { JobFilters, makeFiltersFromQueryString, makeQueryString } from "./JobsContainer";

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
    const queryString = makeQueryString(filters)
    assertStringHasQueryParams(["queue=test", "newest_first=false"], queryString)
  })

  test("makes string with job set", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "job-set",
      jobStates: [],
      newestFirst: false,
    }
    const queryString = makeQueryString(filters)
    assertStringHasQueryParams(["job_set=job-set", "newest_first=false"], queryString)
  })

  test("makes string with single job state", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "",
      jobStates: ["Queued"],
      newestFirst: false,
    }
    const queryString = makeQueryString(filters)
    assertStringHasQueryParams(["job_states=Queued", "newest_first=false"], queryString)
  })

  test("makes string with multiple job states", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "",
      jobStates: ["Queued", "Running", "Cancelled"],
      newestFirst: false,
    }
    const queryString = makeQueryString(filters)
    assertStringHasQueryParams(["job_states=Queued,Running,Cancelled", "newest_first=false"], queryString)
  })

  test("makes string with ordering", () => {
    const filters: JobFilters = {
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    }
    const queryString = makeQueryString(filters)
    assertStringHasQueryParams(["newest_first=true"], queryString)
  })

  test("makes string with all filters", () => {
    const filters: JobFilters = {
      queue: "other-test",
      jobSet: "other-job-set",
      jobStates: ["Pending", "Succeeded", "Failed"],
      newestFirst: true,
    }
    const queryString = makeQueryString(filters)
    assertStringHasQueryParams([
      "queue=other-test",
      "job_set=other-job-set",
      "job_states=Pending,Succeeded,Failed",
      "newest_first=true",
    ], queryString)
  })
})

describe("makeFiltersFromQueryString", () => {
  test("empty string returns default filters", () => {
    const query = ""
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    })
  })

  test("makes filter with queue", () => {
    const query = "queue=test"
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "test",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    })
  })

  test("makes filter with job set", () => {
    const query = "job_set=job-set"
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "job-set",
      jobStates: [],
      newestFirst: true,
    })
  })

  test("makes filter with single job state", () => {
    const query = "job_states=Queued"
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "",
      jobStates: ["Queued"],
      newestFirst: true,
    })
  })

  test("makes filter with multiple job states", () => {
    const query = "job_states=Queued,Pending,Running"
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "",
      jobStates: ["Queued", "Pending", "Running"],
      newestFirst: true,
    })
  })

  const orderingsCases = [["newest_first=true", true], ["newest_first=false", false]]
  test.each(orderingsCases)("makes filter with ordering %p", (query, expectedOrdering) => {
    const filters = makeFiltersFromQueryString(query as string)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: expectedOrdering as boolean,
    })
  })

  test("makes filter with everything", () => {
    const query = "queue=test&job_set=job-set-1&job_states=Queued,Succeeded,Pending&newest_first=false"
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "test",
      jobSet: "job-set-1",
      jobStates: ["Queued", "Succeeded", "Pending"],
      newestFirst: false,
    })
  })

  const nonExistentJobStatesCases = [
    ["job_states=SomethingElse", []],
    ["job_states=Cancelled,SomethingElse,Succeeded,Failed", ["Cancelled", "Succeeded", "Failed"]],
  ]
  test.each(nonExistentJobStatesCases)("non existent job states are ignored %p", (query, expectedJobStates) => {
    const filters = makeFiltersFromQueryString(query as string)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "",
      jobStates: expectedJobStates,
      newestFirst: true,
    })
  })

  test("other query parameters are ignored", () => {
    const query = "something=else"
    const filters = makeFiltersFromQueryString(query)
    expect(filters).toStrictEqual({
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    })
  })
})
