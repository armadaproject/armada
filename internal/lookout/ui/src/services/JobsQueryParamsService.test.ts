import { v4 as uuidv4 } from "uuid"

import { makeQueryString, updateColumnsFromQueryString } from "./JobsQueryParamsService"

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
        urlParamKey: "queue",
        isDisabled: false,
        filter: "test",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["queue=test"], queryString)
  })

  test("makes string with filter with space", () => {
    const columns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        urlParamKey: "queue",
        isDisabled: false,
        filter: "test ",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["queue=test"], queryString)
  })

  test("makes string with job set", () => {
    const columns = [
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        urlParamKey: "job_set",
        isDisabled: false,
        filter: "test-job-set",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["job_set=test-job-set"], queryString)
  })

  test("makes string with job set with space in filter", () => {
    const columns = [
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        urlParamKey: "job_set",
        isDisabled: false,
        filter: "test-job-set ",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["job_set=test-job-set"], queryString)
  })

  test("makes string with owner", () => {
    const columns = [
      {
        id: "owner",
        name: "owner",
        accessor: "owner",
        urlParamKey: "owner",
        isDisabled: false,
        filter: "test-owner",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["owner=test-owner"], queryString)
  })

  test("makes string with owner with space in filter", () => {
    const columns = [
      {
        id: "owner",
        name: "owner",
        accessor: "owner",
        urlParamKey: "owner",
        isDisabled: false,
        filter: "test-owner ",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["owner=test-owner"], queryString)
  })

  test("makes string with single job state", () => {
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        urlParamKey: "job_states",
        isDisabled: false,
        filter: ["Queued"],
        defaultFilter: [],
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["job_states=Queued"], queryString)
  })

  test("makes string with multiple job states", () => {
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        urlParamKey: "job_states",
        isDisabled: false,
        filter: ["Queued", "Running", "Cancelled"],
        defaultFilter: [],
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["job_states=Queued,Running,Cancelled"], queryString)
  })

  test("makes string with ordering", () => {
    const columns = [
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        urlParamKey: "newest_first",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
        width: 1,
      },
    ]
    const queryString = makeQueryString(columns, [])
    assertStringHasQueryParams(["newest_first=true"], queryString)
  })

  test("makes string with annotation", () => {
    const annotationColumns = [
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-1",
        accessor: "gresearch.co.uk/hyperparameter-1",
        urlParamKey: "gresearch.co.uk/hyperparameter-1",
        isDisabled: false,
        filter: "1e-4",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString([], annotationColumns)
    assertStringHasQueryParams(["gresearch.co.uk%2Fhyperparameter-1=1e-4"], queryString)
  })

  test("makes string with multiple annotations", () => {
    const annotationColumns = [
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-1",
        accessor: "gresearch.co.uk/hyperparameter-1",
        urlParamKey: "gresearch.co.uk/hyperparameter-1",
        isDisabled: false,
        filter: "1e-4",
        defaultFilter: "",
        width: 1,
      },
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-2",
        accessor: "gresearch.co.uk/hyperparameter-2",
        urlParamKey: "gresearch.co.uk/hyperparameter-2",
        isDisabled: false,
        filter: "50",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString([], annotationColumns)
    assertStringHasQueryParams(
      ["gresearch.co.uk%2Fhyperparameter-1=1e-4", "gresearch.co.uk%2Fhyperparameter-2=50"],
      queryString,
    )
  })

  test("makes string with all filters", () => {
    const defaultColumns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        urlParamKey: "queue",
        isDisabled: false,
        filter: "other-test",
        defaultFilter: "",
        width: 1,
      },
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        urlParamKey: "job_set",
        isDisabled: false,
        filter: "other-job-set",
        defaultFilter: "",
        width: 1,
      },
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        urlParamKey: "job_states",
        isDisabled: false,
        filter: ["Pending", "Succeeded", "Failed"],
        defaultFilter: [],
        width: 1,
      },
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        urlParamKey: "newest_first",
        isDisabled: false,
        filter: false,
        defaultFilter: true,
        width: 1,
      },
    ]
    const annotationColumns = [
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-1",
        accessor: "gresearch.co.uk/hyperparameter-1",
        urlParamKey: "gresearch.co.uk/hyperparameter-1",
        isDisabled: false,
        filter: "1e-4",
        defaultFilter: "",
        width: 1,
      },
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-2",
        accessor: "gresearch.co.uk/hyperparameter-2",
        urlParamKey: "gresearch.co.uk/hyperparameter-2",
        isDisabled: false,
        filter: "50",
        defaultFilter: "",
        width: 1,
      },
    ]
    const queryString = makeQueryString(defaultColumns, annotationColumns)
    assertStringHasQueryParams(
      [
        "queue=other-test",
        "job_set=other-job-set",
        "job_states=Pending,Succeeded,Failed",
        "newest_first=false",
        "gresearch.co.uk%2Fhyperparameter-1=1e-4",
        "gresearch.co.uk%2Fhyperparameter-2=50",
      ],
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
        urlParamKey: "queue",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query, columns, [])
    expect(columns[0].filter).toEqual("test")
  })

  test("updates job set", () => {
    const query = "job_set=test-job-set"
    const columns = [
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        urlParamKey: "job_set",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query, columns, [])
    expect(columns[0].filter).toEqual("test-job-set")
  })

  test("updates job states with single", () => {
    const query = "job_states=Queued"
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        urlParamKey: "job_states",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query, columns, [])
    expect(columns[0].filter).toStrictEqual(["Queued"])
  })

  test("updates job states with multiple", () => {
    const query = "job_states=Queued,Pending,Running"
    const columns = [
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        urlParamKey: "job_states",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query, columns, [])
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
        urlParamKey: "newest_first",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query as string, columns, [])
    expect(columns[0].filter).toEqual(expectedOrdering as boolean)
  })

  test("updates annotations", () => {
    const query = "gresearch.co.uk%2Fhyperparameter-1=one&gresearch.co.uk%2Fhyperparameter-2=two"
    const annotationColumns = [
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-1",
        accessor: "gresearch.co.uk/hyperparameter-1",
        urlParamKey: "gresearch.co.uk/hyperparameter-1",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-2",
        accessor: "gresearch.co.uk/hyperparameter-2",
        urlParamKey: "gresearch.co.uk/hyperparameter-2",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query, [], annotationColumns)
    expect(annotationColumns[0].filter).toEqual("one")
    expect(annotationColumns[1].filter).toEqual("two")
  })

  test("updates many columns", () => {
    const query =
      "queue=test&job_set=job-set-1&gresearch.co.uk%2Fhyperparameter-1=one&gresearch.co.uk%2Fhyperparameter-2=two&job_states=Queued,Succeeded,Pending&newest_first=false"
    const defaultColumns = [
      {
        id: "queue",
        name: "queue",
        accessor: "queue",
        urlParamKey: "queue",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
      {
        id: "jobSet",
        name: "jobSet",
        accessor: "jobSet",
        urlParamKey: "job_set",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
      {
        id: "jobState",
        name: "jobState",
        accessor: "jobState",
        urlParamKey: "job_states",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
        width: 1,
      },
      {
        id: "submissionTime",
        name: "submissionTime",
        accessor: "submissionTime",
        urlParamKey: "newest_first",
        isDisabled: false,
        filter: true,
        defaultFilter: true,
        width: 1,
      },
    ]
    const annotationColumns = [
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-1",
        accessor: "gresearch.co.uk/hyperparameter-1",
        urlParamKey: "gresearch.co.uk/hyperparameter-1",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
      {
        id: uuidv4(),
        name: "gresearch.co.uk/hyperparameter-2",
        accessor: "gresearch.co.uk/hyperparameter-2",
        urlParamKey: "gresearch.co.uk/hyperparameter-2",
        isDisabled: false,
        filter: "",
        defaultFilter: "",
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query, defaultColumns, annotationColumns)
    expect(defaultColumns[0].filter).toEqual("test")
    expect(defaultColumns[1].filter).toEqual("job-set-1")
    expect(defaultColumns[2].filter).toStrictEqual(["Queued", "Succeeded", "Pending"])
    expect(defaultColumns[3].filter).toEqual(false)
    expect(annotationColumns[0].filter).toEqual("one")
    expect(annotationColumns[1].filter).toEqual("two")
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
        urlParamKey: "job_states",
        isDisabled: false,
        filter: [],
        defaultFilter: [],
        width: 1,
      },
    ]
    updateColumnsFromQueryString(query as string, columns, [])
    expect(columns[0].filter).toStrictEqual(expectedJobStates)
  })
})
