import { isJobSetsView, JobSetsContainerState } from "../containers/JobSetsContainer"

const LOCAL_STORAGE_KEY = "armada_lookout_job_sets_user_settings"

export type JobSetsLocalStorageState = {
  autoRefresh?: boolean
  queue?: string
  currentView?: string
  newestFirst?: boolean
  activeOnly?: boolean
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

    const loadedState = JSON.parse(stateJson) as JobSetsLocalStorageState
    if (loadedState.autoRefresh != undefined) state.autoRefresh = loadedState.autoRefresh
    if (loadedState.queue) state.queue = loadedState.queue
    if (loadedState.currentView && isJobSetsView(loadedState.currentView)) state.currentView = loadedState.currentView
    if (loadedState.newestFirst != undefined) state.newestFirst = loadedState.newestFirst
    if (loadedState.activeOnly != undefined) state.activeOnly = loadedState.activeOnly
  }
}
