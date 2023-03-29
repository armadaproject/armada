import { ExpandedStateList, ColumnFiltersState, SortingState, VisibilityState } from "@tanstack/react-table"
import { JobId } from "models/lookoutV2Models"
import qs from "qs"

import { Router } from "../../utils"
import {
  ANNOTATION_COLUMN_PREFIX,
  ColumnId,
  createAnnotationColumn,
  DEFAULT_COLUMN_VISIBILITY,
  DEFAULT_GROUPING,
  JobTableColumn,
  JOB_COLUMNS,
  DEFAULT_FILTERS,
} from "../../utils/jobsTableColumns"

export interface JobsTablePreferences {
  allColumnsInfo: JobTableColumn[]
  visibleColumns: VisibilityState
  groupedColumns: ColumnId[]
  expandedState: ExpandedStateList
  pageIndex: number
  pageSize: number
  sortingState: SortingState
  columnSizing?: Record<string, number>
  filterState: ColumnFiltersState
  sidebarJobId: JobId | undefined
  sidebarWidth?: number
}

// Need two 'defaults'
export const BLANK_PREFERENCES: JobsTablePreferences = {
  allColumnsInfo: JOB_COLUMNS,
  visibleColumns: DEFAULT_COLUMN_VISIBILITY,
  filterState: [],
  groupedColumns: [],
  expandedState: {},
  pageIndex: 0,
  pageSize: 50,
  sortingState: [{ id: "jobId", desc: true }],
  sidebarJobId: undefined,
  columnSizing: {},
}

export const DEFAULT_QUERY_PARAM_PREFERENCES: Partial<JobsTablePreferences> = {
  filterState: DEFAULT_FILTERS,
  groupedColumns: DEFAULT_GROUPING,
}

export const DEFAULT_LOCAL_STORAGE_PREFERENCES: Partial<JobsTablePreferences> = {
  sidebarWidth: 600,
}

const KEY_PREFIX = "lookoutV2"
const COLUMN_SIZING_KEY = `${KEY_PREFIX}ColumnSizing`
const SIDEBAR_WIDTH_KEY = `${KEY_PREFIX}SidebarWidth`

// Reflects the type of data stored in the URL query params
// Keys are shortened to keep URL size lower
interface QueryStringSafePrefs {
  // Visible columns
  vCols: string[]

  // Annotation keys added
  aCols: string[]

  // Grouped columns
  g: string[] | [null]

  // Expanded rows
  e: string[]

  // Current page number
  page: string

  // Page size
  pS: string

  // Sorting information
  sort: {
    id: string
    desc: string // boolean
  }[]

  // Column filters
  f: {
    id: string
    value: string
  }[]

  // Sidebar job ID
  sb: string | undefined
}

const toQueryStringSafe = (prefs: JobsTablePreferences): QueryStringSafePrefs => {
  // QS lib will remove params with empty array values unless they explicitly have a null value
  // This is useful where the default value is non-empty
  const padEmptyArrayWithNull = <T>(arr: T[]): T[] | [null] => (arr.length === 0 ? [null] : arr)

  // The order of these keys are the order they'll show in the URL bar (in modern browsers)
  return {
    page: prefs.pageIndex.toString(),

    g: padEmptyArrayWithNull(prefs.groupedColumns),

    f: prefs.filterState.map(({ id, value }) => ({ id, value: value as string })),

    sort: prefs.sortingState.map(({ id, desc }) => ({ id, desc: desc.toString() })),

    vCols: Object.entries(prefs.visibleColumns)
      .filter(([_, visible]) => visible)
      .map(([columnId]) => columnId),

    aCols: prefs.allColumnsInfo
      .filter((col) => col.id?.startsWith(ANNOTATION_COLUMN_PREFIX))
      .map((col) => col.id?.slice(ANNOTATION_COLUMN_PREFIX.length))
      .filter((annotationKey): annotationKey is string => annotationKey !== undefined),

    e: Object.entries(prefs.expandedState)
      .filter(([_, expanded]) => expanded)
      .map(([rowId, _]) => rowId),

    pS: prefs.pageSize.toString(),

    sb: prefs.sidebarJobId,
  }
}

const fromQueryStringSafe = (serializedPrefs: Partial<QueryStringSafePrefs>): Partial<JobsTablePreferences> => {
  const stripNullArrays = <T>(arr: T[] | [null]): T[] => (arr.length === 1 && arr[0] === null ? [] : (arr as T[]))

  const { aCols, vCols, g, e, page, pS, sort, f, sb } = serializedPrefs
  const allColumns = JOB_COLUMNS.concat((aCols ?? []).map((annotationKey) => createAnnotationColumn(annotationKey)))

  return {
    ...(aCols && { allColumnsInfo: allColumns }),

    ...(vCols && {
      visibleColumns: Object.fromEntries(allColumns.map(({ id }) => [id, vCols.includes(id as string)])),
    }),

    ...(g && { groupedColumns: stripNullArrays(g) as ColumnId[] }),

    ...(e && { expandedState: Object.fromEntries(e.map((rowId) => [rowId, true])) }),

    ...(page !== undefined && { pageIndex: Number(page) }),

    ...(pS !== undefined && { pageSize: Number(pS) }),

    ...(sort && { sortingState: sort.map((field) => ({ id: field.id, desc: field.desc === "true" })) }),

    ...(f && { filterState: f }),

    ...(sb && { sidebarJobId: sb }),
  }
}

export class JobsTablePreferencesService {
  constructor(private router: Router) {}

  getUserPrefs(): JobsTablePreferences {
    let queryParamPrefs = this.getPrefsFromQueryParams()
    let localStoragePrefs = this.getPrefsFromLocalStorage()
    if (allFieldsAreUndefined(queryParamPrefs)) {
      queryParamPrefs = DEFAULT_QUERY_PARAM_PREFERENCES
    }
    if (allFieldsAreUndefined(localStoragePrefs)) {
      localStoragePrefs = DEFAULT_LOCAL_STORAGE_PREFERENCES
    }
    return {
      ...BLANK_PREFERENCES,
      ...queryParamPrefs,
      ...localStoragePrefs,
    }
  }

  saveNewPrefs(newPrefs: JobsTablePreferences) {
    this.savePrefsToQueryParams(newPrefs)
    this.savePrefsToLocalStorage(newPrefs)
  }

  private savePrefsToQueryParams(newPrefs: JobsTablePreferences) {
    try {
      // Avoids overwriting existing unrelated query params
      const existingQueryParams = qs.parse(this.router.location.search, { ignoreQueryPrefix: true })
      const prefsQueryParams = toQueryStringSafe(newPrefs)
      const mergedQueryParams = {
        ...existingQueryParams,
        ...prefsQueryParams,
      }

      this.router.navigate({
        pathname: this.router.location.pathname,
        search: qs.stringify(mergedQueryParams, {
          encodeValuesOnly: true,
          strictNullHandling: true,
        }),
      })
    } catch (e) {
      console.warn("Unable to update URL query params with table state:", e)
    }
  }

  private savePrefsToLocalStorage(newPrefs: JobsTablePreferences) {
    this.saveColumnSizingToLocalStorage(newPrefs.columnSizing)
    this.saveSidebarWidthToLocalStorage(newPrefs.sidebarWidth)
  }

  private saveColumnSizingToLocalStorage(columnSizing?: Record<string, number>) {
    if (columnSizing) {
      localStorage.setItem(COLUMN_SIZING_KEY, JSON.stringify(columnSizing))
    }
  }

  private saveSidebarWidthToLocalStorage(sidebarWidth?: number) {
    if (sidebarWidth === undefined || sidebarWidth === 0) {
      localStorage.removeItem(SIDEBAR_WIDTH_KEY)
    }
    localStorage.setItem(SIDEBAR_WIDTH_KEY, JSON.stringify(sidebarWidth))
  }

  private getPrefsFromQueryParams(): Partial<JobsTablePreferences> {
    try {
      const queryParamPrefs = qs.parse(this.router.location.search, {
        ignoreQueryPrefix: true,
        strictNullHandling: true,
      })
      return fromQueryStringSafe(queryParamPrefs)
    } catch (e) {
      console.warn("Unable to parse URL query params:", e)
      return {}
    }
  }

  private getPrefsFromLocalStorage(): Partial<JobsTablePreferences> {
    return {
      columnSizing: this.getColumnSizingFromLocalStorage(),
      sidebarWidth: this.getSidebarWidthFromLocalStorage(),
    }
  }

  private getColumnSizingFromLocalStorage(): Record<string, number> | undefined {
    const json = localStorage.getItem(COLUMN_SIZING_KEY)
    if (stringIsInvalid(json)) {
      return undefined
    }

    const obj = tryParseJson(json as string)
    if (!obj) {
      return undefined
    }

    const ans: Record<string, number> = {}
    for (const key in obj) {
      const val = obj[key]
      if (typeof val === "number") {
        ans[key] = val
      }
    }
    return ans
  }

  private getSidebarWidthFromLocalStorage(): number | undefined {
    const json = localStorage.getItem(SIDEBAR_WIDTH_KEY)
    if (stringIsInvalid(json)) {
      return undefined
    }

    const obj = tryParseJson(json as string)
    if (!obj) {
      return undefined
    }
    return typeof obj === "number" ? obj : undefined
  }
}

function stringIsInvalid(s: string | undefined | null): boolean {
  return s === undefined || s === null || s.length === 0 || s === "undefined"
}

function allFieldsAreUndefined(obj: Record<string, unknown>): boolean {
  return Object.values(obj).every((el) => el === undefined)
}

function tryParseJson(json: string): any | undefined {
  try {
    return JSON.parse(json) as Record<string, unknown>
  } catch (e: unknown) {
    if (e instanceof Error) {
      console.warn(e.message)
    }
    return undefined
  }
}
