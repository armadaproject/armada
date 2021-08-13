import { OverviewContainerState } from "../containers/OverviewContainer"

const LOCAL_STORAGE_KEY = "armada_lookout_overview_user_settings"

export type OverviewLocalStorageState = {
  autoRefresh?: boolean
}

export default class OverviewLocalStorageService {
  saveState(state: OverviewContainerState) {
    const localStorageState = {
      autoRefresh: state.autoRefresh,
    }
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(localStorageState))
  }

  updateState(state: OverviewContainerState) {
    const stateJson = localStorage.getItem(LOCAL_STORAGE_KEY)
    if (stateJson == undefined) {
      return
    }

    const loadedState = JSON.parse(stateJson) as OverviewLocalStorageState
    if (loadedState.autoRefresh != undefined) state.autoRefresh = loadedState.autoRefresh
  }
}
