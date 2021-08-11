import { ColumnSpec, JobsContainerState } from "../containers/JobsContainer"

const LOCAL_STORAGE_KEY = "armada_lookout_jobs_user_state"

export type JobsLocalStorageState = {
  autoRefresh?: boolean
  defaultColumns?: ColumnSpec<string | boolean | string[]>[]
  annotationColumns?: ColumnSpec<string>[]
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

    const loadedState = JSON.parse(stateJson) as JobsLocalStorageState
    if (loadedState.autoRefresh != undefined) state.autoRefresh = loadedState.autoRefresh
    if (loadedState.defaultColumns) state.defaultColumns = loadedState.defaultColumns
    if (loadedState.annotationColumns) state.annotationColumns = loadedState.annotationColumns
  }
}
