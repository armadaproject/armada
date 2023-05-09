import { tryParseJson } from "../../utils"
import {
  DEFAULT_PREFERENCES,
  ensurePreferencesAreConsistent,
  JobsTablePreferences,
  KEY_PREFIX,
  stringIsInvalid,
} from "./JobsTablePreferencesService"

const CUSTOM_KEY_PREFIX = `${KEY_PREFIX}CustomPrefs_`
const CUSTOM_PREFS_LIST_KEY = `${KEY_PREFIX}ListCustomPrefs`

const getKey = (name: string): string => {
  return `${CUSTOM_KEY_PREFIX}${name}`
}

export class CustomConfigError extends Error {
  constructor(msg: string) {
    super(msg)
    Object.setPrototypeOf(this, CustomConfigError.prototype)
  }
}

export class CustomPreferencesService {
  savePrefs(name: string, prefs: JobsTablePreferences) {
    const key = getKey(name)
    const existingPrefs = this.getAllPrefNames()
    if (!existingPrefs.includes(name)) {
      existingPrefs.push(name)
      localStorage.setItem(CUSTOM_PREFS_LIST_KEY, JSON.stringify(existingPrefs))
    }
    localStorage.setItem(key, JSON.stringify(prefs))
  }

  loadPrefs(name: string): JobsTablePreferences {
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
      obj,
    }
    ensurePreferencesAreConsistent(prefs)
    return prefs
  }

  deletePrefs(name: string) {
    const key = getKey(name)
    localStorage.removeItem(key)
    const existingPrefs = this.getAllPrefNames()
    const index = existingPrefs.indexOf(name)
    if (index > -1) {
      existingPrefs.splice(index, 1)
      localStorage.setItem(CUSTOM_PREFS_LIST_KEY, JSON.stringify(existingPrefs))
    }
  }

  getAllPrefNames(): string[] {
    const json = localStorage.getItem(CUSTOM_PREFS_LIST_KEY)
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
