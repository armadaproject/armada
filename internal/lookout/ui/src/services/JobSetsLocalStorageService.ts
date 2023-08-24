import { isJobSetsView, JobSetsContainerState } from "../containers/JobSetsContainer"
import { tryParseJson } from "../utils"

const LOCAL_STORAGE_KEY = "armada_lookout_job_sets_user_settings"

export type JobSetsLocalStorageState = {
  autoRefresh?: boolean
  queue?: string
  currentView?: string
  newestFirst?: boolean
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
  if (
    loadedData.currentView != undefined &&
    typeof loadedData.currentView == "string" &&
    isJobSetsView(loadedData.currentView)
  ) {
    state.currentView = loadedData.currentView
  }
  if (loadedData.newestFirst != undefined && typeof loadedData.newestFirst == "boolean") {
    state.newestFirst = loadedData.newestFirst
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
      currentView: state.currentView,
      newestFirst: state.newestFirst,
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
    if (loadedState.currentView && isJobSetsView(loadedState.currentView)) state.currentView = loadedState.currentView
    if (loadedState.newestFirst != undefined) state.newestFirst = loadedState.newestFirst
    if (loadedState.activeOnly != undefined) state.activeOnly = loadedState.activeOnly
  }
}
