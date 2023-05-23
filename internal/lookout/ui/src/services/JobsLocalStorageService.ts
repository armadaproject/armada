import { ColumnSpec, isColumnSpec, JobsContainerState } from "../containers/JobsContainer"
import { tryParseJson } from "../utils"

const LOCAL_STORAGE_KEY = "armada_lookout_jobs_user_settings"

export type JobsLocalStorageState = {
  autoRefresh?: boolean
  defaultColumns?: ColumnSpec<string | boolean | string[]>[]
  annotationColumns?: ColumnSpec<string>[]
}

function isColumnArray(data: unknown): boolean {
  return data !== undefined && Array.isArray(data) && data.every((col: any) => isColumnSpec(col))
}

function loadedDataIsValid(loadedData: Record<string, unknown>): boolean {
  return (
    loadedData.autoRefresh !== undefined &&
    typeof loadedData.autoRefresh == "boolean" &&
    isColumnArray(loadedData.defaultColumns) &&
    isColumnArray(loadedData.annotationColumns)
  )
}

export function convertToLocalStorageState(loadedData: Record<string, unknown>): [JobsLocalStorageState, boolean] {
  const state: JobsLocalStorageState = {}

  if (!loadedDataIsValid(loadedData)) {
    return [state, false]
  }

  state.autoRefresh = loadedData.autoRefresh as boolean
  state.defaultColumns = loadedData.defaultColumns as ColumnSpec<string | boolean | string[]>[]
  state.annotationColumns = loadedData.annotationColumns as ColumnSpec<string>[]

  return [state, true]
}

function resetFilter<T>(col: ColumnSpec<T>): ColumnSpec<T> {
  col.filter = col.defaultFilter
  return col
}

export default class JobsLocalStorageService {
  saveState(state: JobsContainerState) {
    const localStorageState = {
      autoRefresh: state.autoRefresh,
      defaultColumns: state.defaultColumns.map((c) => ({ ...c })).map((c) => resetFilter(c)),
      annotationColumns: state.annotationColumns.map((c) => ({ ...c })).map((c) => resetFilter(c)),
    }
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(localStorageState))
  }

  updateState(state: JobsContainerState) {
    const stateJson = localStorage.getItem(LOCAL_STORAGE_KEY)
    if (stateJson == undefined) {
      return
    }

    const loadedData = tryParseJson(stateJson)
    if (loadedData == undefined) {
      // Bad JSON - clear it from local storage
      localStorage.removeItem(LOCAL_STORAGE_KEY)
      return
    }

    const [loadedState, ok] = convertToLocalStorageState(loadedData as Record<string, unknown>)
    if (!ok) {
      // Couldn't convert local storage data to columns - clear it
      localStorage.removeItem(LOCAL_STORAGE_KEY)
      return
    }

    if (loadedState.autoRefresh !== undefined) state.autoRefresh = loadedState.autoRefresh
    if (loadedState.defaultColumns) state.defaultColumns = loadedState.defaultColumns
    if (loadedState.annotationColumns) state.annotationColumns = loadedState.annotationColumns
  }
}
