import { v4 as uuidv4 } from "uuid"

import { convertToLocalStorageState } from "./JobsLocalStorageService"

describe("convertToLocalStorageState", () => {
  test("does not convert empty object", () => {
    const data = {}
    const [, ok] = convertToLocalStorageState(data)
    expect(ok).toBe(false)
  })

  test("does not convert if not all fields are present", () => {
    const data = {
      autoRefresh: false,
      defaultColumns: [],
    }
    const [, ok] = convertToLocalStorageState(data)
    expect(ok).toBe(false)
  })

  test("converts if columns are empty", () => {
    const data = {
      autoRefresh: true,
      defaultColumns: [],
      annotationColumns: [],
    }
    const [state, ok] = convertToLocalStorageState(data)
    expect(ok).toBe(true)
    expect(state.autoRefresh).toBe(true)
    expect(state.defaultColumns).toEqual([])
    expect(state.annotationColumns).toEqual([])
  })

  test("does not convert if a column is of the wrong format", () => {
    const col = {
      id: "queue",
      name: "queue",
      accessor: "queue",
      // missing urlParamKey
      isDisabled: false,
      filter: "test",
      defaultFilter: "",
      width: 1,
    }
    const data = {
      autoRefresh: true,
      defaultColumns: [col],
      annotationColumns: [],
    }
    const [, ok] = convertToLocalStorageState(data)
    expect(ok).toBe(false)
  })

  test("converts if columns are of correct format", () => {
    const col = {
      id: "queue",
      name: "queue",
      accessor: "queue",
      urlParamKey: "queue",
      isDisabled: false,
      filter: "test",
      defaultFilter: "",
      width: 1,
    }
    const annotationCol = {
      id: uuidv4(),
      name: "gresearch.co.uk/hyperparameter-2",
      accessor: "gresearch.co.uk/hyperparameter-2",
      urlParamKey: "gresearch.co.uk/hyperparameter-2",
      isDisabled: false,
      filter: "50",
      defaultFilter: "",
      width: 1,
    }
    const data = {
      autoRefresh: true,
      defaultColumns: [col],
      annotationColumns: [annotationCol],
    }
    const [state, ok] = convertToLocalStorageState(data)
    expect(ok).toBe(true)
    expect(state.autoRefresh).toBe(true)
    expect(state.defaultColumns).toHaveLength(1)
    expect((state.defaultColumns as any[])[0]).toEqual(col)
    expect(state.annotationColumns).toHaveLength(1)
    expect((state.annotationColumns as any[])[0]).toEqual(annotationCol)
  })
})
