import { OverviewContainerState } from "../containers/OverviewContainer"
import { tryParseJson } from "../utils"

const LOCAL_STORAGE_KEY = "armada_lookout_overview_user_settings"

export type OverviewLocalStorageState = {
  autoRefresh?: boolean
}

function convertToLocalStorageState(loadedData: Record<string, unknown>): OverviewLocalStorageState {
  const state: OverviewLocalStorageState = {}

  if (loadedData.autoRefresh != undefined && typeof loadedData.autoRefresh == "boolean") {
    state.autoRefresh = loadedData.autoRefresh
  }

  return state
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

    const loadedData = tryParseJson(stateJson)
    if (loadedData == undefined) {
      return
    }

    const loadedState = convertToLocalStorageState(loadedData as Record<string, unknown>)
    if (loadedState.autoRefresh != undefined) state.autoRefresh = loadedState.autoRefresh
  }
}
