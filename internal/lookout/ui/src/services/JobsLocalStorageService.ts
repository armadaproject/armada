import { ColumnSpec, isColumnSpec, JobsContainerState } from "../containers/JobsContainer"
import { tryParseJson } from "../utils"

const LOCAL_STORAGE_KEY = "armada_lookout_jobs_user_settings"

export type JobsLocalStorageState = {
  autoRefresh?: boolean
  defaultColumns?: ColumnSpec<string | boolean | string[]>[]
  annotationColumns?: ColumnSpec<string>[]
}

function convertToLocalStorageState(loadedData: Record<string, unknown>): JobsLocalStorageState {
  const state: JobsLocalStorageState = {}

  if (loadedData.autoRefresh != undefined && typeof loadedData.autoRefresh == "boolean") {
    state.autoRefresh = loadedData.autoRefresh
  }
  if (
    loadedData.defaultColumns != undefined &&
    Array.isArray(loadedData.defaultColumns) &&
    loadedData.defaultColumns.every((col: any) => isColumnSpec(col))
  ) {
    state.defaultColumns = loadedData.defaultColumns
  }
  if (
    loadedData.annotationColumns != undefined &&
    Array.isArray(loadedData.annotationColumns) &&
    loadedData.annotationColumns.every((col: any) => isColumnSpec(col))
  ) {
    state.annotationColumns = loadedData.annotationColumns
  }

  return state
}

export default class JobsLocalStorageService {
  saveState(state: JobsContainerState) {
    const localStorageState = {
      autoRefresh: state.autoRefresh,
      defaultColumns: state.defaultColumns,
      annotationColumns: state.annotationColumns,
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
      return
    }

    const loadedState = convertToLocalStorageState(loadedData)
    if (loadedState.autoRefresh != undefined) state.autoRefresh = loadedState.autoRefresh
    if (loadedState.defaultColumns) state.defaultColumns = loadedState.defaultColumns
    if (loadedState.annotationColumns) state.annotationColumns = loadedState.annotationColumns
  }
}
