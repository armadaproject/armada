import { makeQueryString, updateColumnsFromQueryString } from "./JobsContainer"

function assertStringHasQueryParams(expected: string[], actual: string) {
  const actualQueryParams = actual.split("&")
  expect(expected.sort()).toStrictEqual(actualQueryParams.sort())
}

describe("makeQueryString", () => {
  test("makes string with queue", () => {
    const columns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        isDisabled: false,
        filter: "test",
        defaultFilter: "",
      },
    ]
    const queryString = makeQueryString(columns)
    assertStringHasQueryParams(["queue=test"], queryString)
  })

  test("makes string with job set", () => {
    const columns = [
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        isDisabled: false,
        filter: "test-job-set",
        defaultFilter: "",
      },
    ]
    const queryString = makeQueryString(columns)
    assertStringHasQueryParams(["job_set=test-job-set"], queryString)
  })

  test("makes string with single job state", () => {
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: ["Queued"],
        defaultFilter: [],
      },
    ]
    const queryString = makeQueryString(columns)
    assertStringHasQueryParams(["job_states=Queued"], queryString)
  })

  test("makes string with multiple job states", () => {
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: ["Queued", "Running", "Cancelled"],
        defaultFilter: [],
      },
    ]
    const queryString = makeQueryString(columns)
    assertStringHasQueryParams(["job_states=Queued,Running,Cancelled"], queryString)
  })

  test("makes string with ordering", () => {
    const columns = [
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
      },
    ]
    const queryString = makeQueryString(columns)
    assertStringHasQueryParams(["newest_first=true"], queryString)
  })

  test("makes string with all filters", () => {
    const columns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        isDisabled: false,
        filter: "other-test",
        defaultFilter: "",
      },
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        isDisabled: false,
        filter: "other-job-set",
        defaultFilter: "",
      },
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: ["Pending", "Succeeded", "Failed"],
        defaultFilter: [],
      },
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
      },
    ]
    const queryString = makeQueryString(columns)
    assertStringHasQueryParams(
      ["queue=other-test", "job_set=other-job-set", "job_states=Pending,Succeeded,Failed", "newest_first=true"],
      queryString,
    )
  })
})

describe("updateColumnsFromQueryString", () => {
  test("updates queue", () => {
    const query = "queue=test"
    const columns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
      },
    ]
    updateColumnsFromQueryString(query, columns)
    expect(columns[0].filter).toEqual("test")
  })

  test("updates job set", () => {
    const query = "job_set=test-job-set"
    const columns = [
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
      },
    ]
    updateColumnsFromQueryString(query, columns)
    expect(columns[0].filter).toEqual("test-job-set")
  })

  test("updates job states with single", () => {
    const query = "job_states=Queued"
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
      },
    ]
    updateColumnsFromQueryString(query, columns)
    expect(columns[0].filter).toStrictEqual(["Queued"])
  })

  test("updates job states with multiple", () => {
    const query = "job_states=Queued,Pending,Running"
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
      },
    ]
    updateColumnsFromQueryString(query, columns)
    expect(columns[0].filter).toStrictEqual(["Queued", "Pending", "Running"])
  })

  const orderingsCases = [
    ["newest_first=true", true],
    ["newest_first=false", false],
  ]
  test.each(orderingsCases)("updates ordering %p", (query, expectedOrdering) => {
    const columns = [
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
      },
    ]
    updateColumnsFromQueryString(query as string, columns)
    expect(columns[0].filter).toEqual(expectedOrdering as boolean)
  })

  test("updates many columns", () => {
    const query = "queue=test&job_set=job-set-1&job_states=Queued,Succeeded,Pending&newest_first=false"
    const columns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
      },
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
      },
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
      },
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
      },
    ]
    updateColumnsFromQueryString(query, columns)
    expect(columns[0].filter).toEqual("test")
    expect(columns[1].filter).toEqual("job-set-1")
    expect(columns[2].filter).toStrictEqual(["Queued", "Succeeded", "Pending"])
    expect(columns[3].filter).toEqual(false)
  })

  const nonExistentJobStatesCases = [
    ["job_states=SomethingElse", []],
    ["job_states=Cancelled,SomethingElse,Succeeded,Failed", ["Cancelled", "Succeeded", "Failed"]],
  ]
  test.each(nonExistentJobStatesCases)("non existent job states are ignored %p", (query, expectedJobStates) => {
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
      },
    ]
    updateColumnsFromQueryString(query as string, columns)
    expect(columns[0].filter).toStrictEqual(expectedJobStates)
  })
})
