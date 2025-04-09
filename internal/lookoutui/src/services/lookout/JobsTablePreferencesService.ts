import { ColumnFiltersState, ExpandedStateList, VisibilityState } from "@tanstack/react-table"
import _ from "lodash"
import qs from "qs"

import { LookoutColumnOrder } from "../../containers/lookout/JobsTableContainer"
import { isValidMatch, JobId, Match } from "../../models/lookoutModels"
import { removeUndefined, Router } from "../../utils"
import {
  AnnotationColumnId,
  ColumnId,
  DEFAULT_COLUMN_MATCHES,
  DEFAULT_COLUMN_ORDERING,
  DEFAULT_COLUMN_VISIBILITY,
  GET_JOB_COLUMNS,
  toAnnotationColId,
  toColId,
  fromAnnotationColId,
  isStandardColId,
  PINNED_COLUMNS,
  StandardColumnId,
} from "../../utils/jobsTableColumns"
import { matchForColumn } from "../../utils/jobsTableUtils"

export interface JobsTablePreferences {
  annotationColumnKeys: string[]
  visibleColumns: VisibilityState
  columnOrder: ColumnId[]
  groupedColumns: ColumnId[]
  expandedState: ExpandedStateList
  pageIndex: number
  pageSize: number
  order: LookoutColumnOrder
  columnSizing?: Record<string, number>
  filters: ColumnFiltersState
  columnMatches: Record<string, Match>
  sidebarJobId: JobId | undefined
  sidebarWidth?: number
  activeJobSets?: boolean
  autoRefresh?: boolean
}

// Need two 'defaults'
export const DEFAULT_PREFERENCES: JobsTablePreferences = {
  annotationColumnKeys: [],
  visibleColumns: DEFAULT_COLUMN_VISIBILITY,
  columnOrder: GET_JOB_COLUMNS({ formatIsoTimestamp: () => "", displayedTimeZoneName: "", formatNumber: () => "" })
    .filter(({ id }) => !PINNED_COLUMNS.includes(toColId(id)))
    .map(({ id }) => toColId(id)),
  filters: [],
  columnMatches: DEFAULT_COLUMN_MATCHES,
  groupedColumns: [],
  expandedState: {},
  pageIndex: 0,
  pageSize: 50,
  order: DEFAULT_COLUMN_ORDERING,
  sidebarJobId: undefined,
  sidebarWidth: 600,
  columnSizing: {},
}

export const KEY_PREFIX = "lookoutV2"
const COLUMN_SIZING_KEY = `${KEY_PREFIX}ColumnSizing`
const SIDEBAR_WIDTH_KEY = `${KEY_PREFIX}SidebarWidth`
export const PREFERENCES_KEY = `${KEY_PREFIX}JobTablePreferences`

type QueryStringJobFilter = {
  id: string
  value: string | string[]
  match: string
}

// Reflects the type of data stored in the URL query params
// Keys are shortened to keep URL size lower
export interface QueryStringPrefs {
  // Grouped columns
  g: string[]
  // Expanded rows
  e: string[]
  // Current page number
  page: string
  // Page size
  ps: string
  // Sorting information
  sort: {
    id: string
    desc: string // boolean
  }
  // Job filters
  f: QueryStringJobFilter[]
  // Sidebar job ID
  sb: string | undefined
  // This is a boolean field, but the qs library turns it into a string.
  active: string | undefined
  // This is a boolean field, but the qs library turns it into a string.
  refresh: string | undefined
}

export const toQueryStringSafe = (prefs: JobsTablePreferences): QueryStringPrefs => {
  // The order of these keys are the order they'll show in the URL bar (in modern browsers)
  return {
    page: prefs.pageIndex.toString(),
    g: prefs.groupedColumns,
    f: prefs.filters.map((filter) => {
      return {
        id: filter.id,
        value: filter.value as string | string[],
        match: matchForColumn(filter.id, prefs.columnMatches),
      }
    }),
    sort: {
      id: prefs.order.id,
      desc: String(prefs.order.direction === "DESC"),
    },
    e: Object.entries(prefs.expandedState)
      .filter(([_, expanded]) => expanded)
      .map(([rowId, _]) => rowId),
    ps: prefs.pageSize.toString(),
    sb: prefs.sidebarJobId,
    active: prefs.activeJobSets === undefined ? undefined : `${prefs.activeJobSets}`,
    refresh: prefs.autoRefresh === undefined ? undefined : `${prefs.autoRefresh}`,
  }
}

const columnFiltersFromQueryStringFilters = (f: QueryStringJobFilter[]): ColumnFiltersState => {
  return f.map((queryFilter) => ({
    id: queryFilter.id,
    value: queryFilter.value,
  }))
}

const columnMatchesFromQueryStringFilters = (f: QueryStringJobFilter[]): Record<string, Match> => {
  const columnMatches: Record<string, Match> = {}
  f.filter((queryFilter) => isValidMatch(queryFilter.match)).forEach((queryFilter) => {
    columnMatches[queryFilter.id] = queryFilter.match as Match
  })
  return columnMatches
}

const fromQueryStringSafe = (serializedPrefs: Partial<QueryStringPrefs>): Partial<JobsTablePreferences> => {
  const { g, e, page, ps, sort, f, sb, active, refresh } = serializedPrefs

  if (f) {
    // The queue filter was a single-value filter, but changed to an any-of filter. If the queue column match is exact,
    // convert it to an equivalent any-of; otherwise remove the filter
    const indiciesToRemove = [] as number[]
    f.forEach(({ id, value, match }, i) => {
      if (id === StandardColumnId.Queue) {
        if (match === Match.Exact || match === Match.AnyOf) {
          f[i].value = _.isArray(value) ? value : [value]
          f[i].match = Match.AnyOf
        } else {
          indiciesToRemove.push(i)
        }
      }
    })
    indiciesToRemove.reverse().forEach((i) => f.splice(i, 1))
  }

  return {
    ...(g && Array.isArray(g) && g.every((a) => typeof a === "string") && { groupedColumns: g as ColumnId[] }),
    ...(e && { expandedState: Object.fromEntries(e.map((rowId) => [rowId, true])) }),
    ...(page !== undefined && { pageIndex: Number(page) }),
    ...(ps !== undefined && { pageSize: Number(ps) }),
    ...(sort && {
      order: { id: sort.id, direction: sort.desc.toLowerCase() === "true" ? "DESC" : "ASC" },
    }),
    ...(f && { filters: columnFiltersFromQueryStringFilters(f) }),
    ...(f && { columnMatches: columnMatchesFromQueryStringFilters(f) }),
    ...(sb && { sidebarJobId: sb }),
    ...(active && { activeJobSets: active.toLowerCase() === "true" }),
    ...(refresh && { autoRefresh: refresh.toLowerCase() === "true" }),
  }
}

const ensureVisible = (visibilityState: VisibilityState, columns: string[]) => {
  for (const col of columns) {
    visibilityState[col] = true
  }
}

// Only field that gets merged from queryParams rather than being completely overridden. This is because we want the
// columns specified in the query params to use the correct column matches, but we do not wish to override the user
// specified column matches for other columns not used in the filters
const mergeColumnMatches = (
  baseColumnMatches: Record<string, Match>,
  newColumnMatches: Record<string, Match> | undefined,
) => {
  if (newColumnMatches === undefined) {
    return
  }
  Object.entries(newColumnMatches).forEach(([id, match]) => {
    baseColumnMatches[id] = match
  })
}

// Use local storage prefs, but if query prefs are defined update all fields managed by query params with their
// corresponding query param ones (even if undefined for some fields)
const mergeQueryParamsAndLocalStorage = (
  queryParamPrefs: Partial<JobsTablePreferences>,
  localStoragePrefs: Partial<JobsTablePreferences>,
): Partial<JobsTablePreferences> => {
  const mergedPrefs: Partial<JobsTablePreferences> = localStoragePrefs
  if (!allFieldsAreUndefined(queryParamPrefs)) {
    // Need to do one by one, as assignment using `keyof` won't be recognized as the same key by Typescript
    mergedPrefs.groupedColumns = queryParamPrefs.groupedColumns
    mergedPrefs.expandedState = queryParamPrefs.expandedState
    mergedPrefs.pageIndex = queryParamPrefs.pageIndex
    mergedPrefs.pageSize = queryParamPrefs.pageSize
    mergedPrefs.order = queryParamPrefs.order
    mergedPrefs.filters = queryParamPrefs.filters
    mergedPrefs.sidebarJobId = queryParamPrefs.sidebarJobId
    if (mergedPrefs.columnMatches === undefined) {
      mergedPrefs.columnMatches = DEFAULT_COLUMN_MATCHES
    }
    mergeColumnMatches(mergedPrefs.columnMatches, queryParamPrefs.columnMatches)
    mergedPrefs.activeJobSets = queryParamPrefs.activeJobSets
    mergedPrefs.autoRefresh = queryParamPrefs.autoRefresh
  }
  return mergedPrefs
}

// Ensure that the match type and the filter value type are consistent
const ensureFiltersAreConsistent = (filters: ColumnFiltersState, columnMatches: Record<string, Match>) => {
  filters.forEach(({ id, value }, i) => {
    const match = columnMatches[id]
    if (match === Match.AnyOf && !_.isArray(value)) {
      // To prevent confusion, we clear the filter completely if the stored value is unexpectedly not an array
      filters[i].value = undefined
    }

    if (match !== Match.AnyOf && _.isArray(value)) {
      // We use the first element of the value array if the stored value is unexpectedly an array
      filters[i].value = value[0]
    }
  })
}

// Ensure that:
// - annotations referenced in filters exist
// - make sure columns referenced in objects are visible
// - the column order includes exactly all unpinned standard columns and annotations
export const ensurePreferencesAreConsistent = (preferences: JobsTablePreferences) => {
  // Make sure annotation columns referenced in filters exist
  if (preferences.annotationColumnKeys === undefined) {
    preferences.annotationColumnKeys = []
  }
  const annotationKeysSet = new Set<string>(preferences.annotationColumnKeys)
  const potentialAnnotations = preferences.filters.map((f) => f.id).concat(preferences.groupedColumns as string[])
  for (const id of potentialAnnotations) {
    if (!isStandardColId(id)) {
      const annotationKey = fromAnnotationColId(id as AnnotationColumnId)
      if (!annotationKeysSet.has(annotationKey)) {
        preferences.annotationColumnKeys.push(annotationKey)
        annotationKeysSet.add(annotationKey)
      }
    }
  }

  // Make sure all and only annotation columns and unpinned standard columns are contained in columnOrder
  const columnOrderSet = new Set(preferences.columnOrder)
  const annotationKeyColumnIds = preferences.annotationColumnKeys.map(toAnnotationColId)
  const unpinnedStandardColumnIds = GET_JOB_COLUMNS({
    formatIsoTimestamp: () => "",
    displayedTimeZoneName: "",
    formatNumber: () => "",
  })
    .filter(({ id }) => !PINNED_COLUMNS.includes(toColId(id)))
    .map(({ id }) => toColId(id))

  // Add missing column IDs
  annotationKeyColumnIds.filter((id) => !columnOrderSet.has(id)).forEach((id) => preferences.columnOrder.push(id))
  unpinnedStandardColumnIds.filter((id) => !columnOrderSet.has(id)).forEach((id) => preferences.columnOrder.push(id))

  // Remove extraneous column IDs
  const annotationKeyColumnIdsSet = new Set(annotationKeyColumnIds)
  const unpinnedStandardColumnIdsSet = new Set(unpinnedStandardColumnIds)
  preferences.columnOrder = preferences.columnOrder.filter(
    (id) => annotationKeyColumnIdsSet.has(id as AnnotationColumnId) || unpinnedStandardColumnIdsSet.has(id),
  )

  // Make sure grouped columns, order columns, and filtered columns are visible
  ensureVisible(preferences.visibleColumns, preferences.groupedColumns ?? [])
  ensureVisible(preferences.visibleColumns, preferences.order === undefined ? [] : [preferences.order.id])
  ensureVisible(preferences.visibleColumns, preferences.filters?.map((filter) => filter.id) ?? [])

  // Ensure filters are consistent
  ensureFiltersAreConsistent(preferences.filters, preferences.columnMatches)
}

export const stringifyQueryParams = (paramObj: any): string =>
  qs.stringify(paramObj, {
    encodeValuesOnly: true,
    strictNullHandling: true,
  })

export class JobsTablePreferencesService {
  constructor(private router: Router) {}

  getUserPrefs(): JobsTablePreferences {
    const queryParamPrefs = this.getPrefsFromQueryParams()
    const localStoragePrefs = this.getPrefsFromLocalStorage()
    const merged = mergeQueryParamsAndLocalStorage(queryParamPrefs, localStoragePrefs)
    removeUndefined(merged)
    const prefs = {
      ...DEFAULT_PREFERENCES,
      ...merged,
    }
    ensurePreferencesAreConsistent(prefs)
    return prefs
  }

  saveNewPrefs(newPrefs: JobsTablePreferences) {
    this.savePrefsToQueryParams(newPrefs)
    this.savePrefsToLocalStorage(newPrefs)
  }

  private savePrefsToQueryParams(newPrefs: JobsTablePreferences) {
    try {
      // Avoids overwriting existing unrelated query params
      const existingQueryParams = qs.parse(this.router.location.search, { ignoreQueryPrefix: true })
      const newQueryParams = toQueryStringSafe(newPrefs)
      const mergedQueryParams = {
        ...existingQueryParams,
        ...newQueryParams,
      }

      this.router.navigate({
        pathname: this.router.location.pathname,
        search: stringifyQueryParams(mergedQueryParams),
      })
    } catch (e) {
      console.warn("Unable to update URL query params with table state:", e)
    }
  }

  private savePrefsToLocalStorage(newPrefs: JobsTablePreferences) {
    localStorage.setItem(PREFERENCES_KEY, JSON.stringify(newPrefs))
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
    const json = localStorage.getItem(PREFERENCES_KEY)
    if (stringIsInvalid(json)) {
      return {}
    }

    const obj = tryParseJson(json as string) as Partial<JobsTablePreferences>
    if (!obj) {
      return {}
    }

    // TODO: needed for backwards compatibility, remove when all users upgraded
    if (obj.columnSizing === undefined || Object.keys(obj.columnSizing).length === 0) {
      obj.columnSizing = this.getColumnSizingFromLocalStorage()
    }
    if (obj.sidebarWidth === undefined || obj.sidebarWidth === 0) {
      obj.sidebarWidth = this.getSidebarWidthFromLocalStorage()
    }

    if (!obj.columnMatches) {
      obj.columnMatches = {}
    }
    if (!obj.filters) {
      obj.filters = []
    }

    // The queue filter was a single-value filter, but changed to an any-of filter. If the queue column match is exact,
    // convert it to an equivalent any-of; otherwise remove the filter
    const existingQueueFilterMatch = obj.columnMatches[StandardColumnId.Queue]
    if (existingQueueFilterMatch && existingQueueFilterMatch !== Match.AnyOf) {
      delete obj.columnMatches[StandardColumnId.Queue]
      if (existingQueueFilterMatch !== Match.Exact) {
        const queueFilterIndex = obj.filters.findIndex(({ id }) => id === StandardColumnId.Queue)
        if (queueFilterIndex >= 0) {
          obj.filters.splice(queueFilterIndex, 1)
        }
      }
    }
    if (obj.filters) {
      obj.filters.forEach(({ id, value }, i) => {
        if (id === StandardColumnId.Queue) {
          obj.columnMatches![StandardColumnId.Queue] = Match.AnyOf
          if (!_.isArray(value)) {
            obj.filters![i].value = [value]
          }
        }
      })
    }

    return obj
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

export function stringIsInvalid(s: string | undefined | null): boolean {
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
