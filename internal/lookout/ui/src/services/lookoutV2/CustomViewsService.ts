import {
  DEFAULT_PREFERENCES,
  ensurePreferencesAreConsistent,
  JobsTablePreferences,
  KEY_PREFIX,
  stringIsInvalid,
} from "./JobsTablePreferencesService"
import { tryParseJson } from "../../utils"

const CUSTOM_KEY_PREFIX = `${KEY_PREFIX}CustomPrefs_`
const CUSTOM_VIEWS_LIST_KEY = `${KEY_PREFIX}ListCustomPrefs`

const getKey = (name: string): string => {
  return `${CUSTOM_KEY_PREFIX}${name}`
}

export class CustomConfigError extends Error {
  constructor(msg: string) {
    super(msg)
    Object.setPrototypeOf(this, CustomConfigError.prototype)
  }
}

export class CustomViewsService {
  saveView(name: string, prefs: JobsTablePreferences) {
    const key = getKey(name)
    const allViews = this.getAllViews()
    if (!allViews.includes(name)) {
      allViews.push(name)
      localStorage.setItem(CUSTOM_VIEWS_LIST_KEY, JSON.stringify(allViews))
    }
    localStorage.setItem(key, JSON.stringify(prefs))
  }

  getView(name: string): JobsTablePreferences {
    const key = getKey(name)
    const json = localStorage.getItem(key)
    if (stringIsInvalid(json)) {
      throw new CustomConfigError(`Loaded config ${name} is invalid`)
    }

    const obj = tryParseJson(json as string) as Partial<JobsTablePreferences>
    if (!obj) {
      throw new CustomConfigError(`Loaded config ${name} is invalid`)
    }
    const prefs = {
      ...DEFAULT_PREFERENCES,
      ...obj,
    }
    ensurePreferencesAreConsistent(prefs)
    return prefs
  }

  deleteView(name: string) {
    const key = getKey(name)
    localStorage.removeItem(key)
    const allViews = this.getAllViews()
    const index = allViews.indexOf(name)
    if (index > -1) {
      allViews.splice(index, 1)
      localStorage.setItem(CUSTOM_VIEWS_LIST_KEY, JSON.stringify(allViews))
    }
  }

  getAllViews(): string[] {
    const json = localStorage.getItem(CUSTOM_VIEWS_LIST_KEY)
    if (stringIsInvalid(json)) {
      return []
    }
    const obj = tryParseJson(json as string)
    if (Array.isArray(obj) && obj.length > 0 && typeof obj[0] === "string") {
      return obj
    }
    return []
  }
}
