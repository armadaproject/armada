import { JobSetsContainerState } from "../containers/JobSetsContainer"
import { tryParseJson } from "../utils"
import { isJobSetsOrderByColumn, JobSetsOrderByColumn } from "./JobService"

const LOCAL_STORAGE_KEY = "armada_lookout_job_sets_user_settings"

export type JobSetsLocalStorageState = {
  autoRefresh?: boolean
  queue?: string
  currentView?: string
  orderByColumn?: JobSetsOrderByColumn
  orderByDesc?: boolean
  activeOnly?: boolean
}

function convertToLocalStorageState(loadedData: Record<string, unknown>): JobSetsLocalStorageState {
  const state: JobSetsLocalStorageState = {}

  if (loadedData.autoRefresh != undefined && typeof loadedData.autoRefresh == "boolean") {
    state.autoRefresh = loadedData.autoRefresh
  }
  if (loadedData.queue != undefined && typeof loadedData.queue == "string") {
    state.queue = loadedData.queue
  }
  if (loadedData.orderByColumn != undefined && isJobSetsOrderByColumn(loadedData.orderByColumn)) {
    state.orderByColumn = loadedData.orderByColumn
  }
  if (loadedData.orderByDesc != undefined && typeof loadedData.orderByDesc == "boolean") {
    state.orderByDesc = loadedData.orderByDesc
  }
  if (loadedData.activeOnly != undefined && typeof loadedData.activeOnly == "boolean") {
    state.activeOnly = loadedData.activeOnly
  }

  return state
}

export default class JobSetsLocalStorageService {
  saveState(state: JobSetsContainerState) {
    const localStorageState = {
      autoRefresh: state.autoRefresh,
      queue: state.queue,
      orderByColumn: state.orderByColumn,
      orderByDesc: state.orderByDesc,
      activeOnly: state.activeOnly,
    }
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(localStorageState))
  }

  updateState(state: JobSetsContainerState) {
    const stateJson = localStorage.getItem(LOCAL_STORAGE_KEY)
    if (stateJson == undefined) {
      return
    }

    const loadedData = tryParseJson(stateJson)
    if (loadedData == undefined) {
      return
    }

    const loadedState = convertToLocalStorageState(loadedData as Record<string, unknown>)
    if (loadedState.autoRefresh != undefined) state.autoRefresh = loadedState.autoRefresh
    if (loadedState.queue) state.queue = loadedState.queue
    if (loadedState.orderByColumn != undefined) state.orderByColumn = loadedState.orderByColumn
    if (loadedState.orderByDesc != undefined) state.orderByDesc = loadedState.orderByDesc
    if (loadedState.activeOnly != undefined) state.activeOnly = loadedState.activeOnly
  }
}
