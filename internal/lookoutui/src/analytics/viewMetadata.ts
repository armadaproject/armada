import { DEFAULT_COLUMN_VISIBILITY, toAnnotationColId } from "../common/jobsTableColumns"
import { Match } from "../models/lookoutModels"
import { JobsTablePreferences } from "../services/lookout/JobsTablePreferencesService"

/**
 * Builds enriched event data from a custom view's preferences.
 * Returns a flat Record<string, string> suitable for analytics event data.
 */
export function buildViewEventData(viewName: string, prefs: JobsTablePreferences): Record<string, string> {
  const visibleCols = Object.entries(prefs.visibleColumns)
    .filter(([, visible]) => visible)
    .map(([id]) => id)

  // Compute column delta from defaults
  const added: string[] = []
  const removed: string[] = []

  for (const [colId, visible] of Object.entries(prefs.visibleColumns)) {
    const isDefault = DEFAULT_COLUMN_VISIBILITY[colId] ?? false
    if (visible && !isDefault) {
      added.push(colId)
    } else if (!visible && isDefault) {
      removed.push(colId)
    }
  }

  // Annotation columns are always "added" (never in defaults)
  const annotationCols = prefs.annotationColumnKeys ?? []
  for (const key of annotationCols) {
    const annotationColId = toAnnotationColId(key)
    if (!added.includes(annotationColId)) {
      added.push(annotationColId)
    }
  }

  const columnsDelta = [...added, ...removed.map((c) => `-${c}`)].join(",")

  const filteredColumns = prefs.filters.map((f) => f.id).join(",")

  const groupedColumns = prefs.groupedColumns.join(",")

  const filterDetails = JSON.stringify(
    prefs.filters.map((f) => ({
      column: f.id,
      matchType: prefs.columnMatches[f.id] ?? Match.Exact,
      value: f.value,
    })),
  )

  return {
    viewName,
    columnCount: String(visibleCols.length),
    columns: columnsDelta,
    filterCount: String(prefs.filters.length),
    filteredColumns,
    groupedColumns,
    groupCount: String(prefs.groupedColumns.length),
    sortColumn: prefs.order.id,
    sortDirection: prefs.order.direction,
    pageSize: String(prefs.pageSize),
    hasAnnotations: String(annotationCols.length > 0),
    annotationCount: String(annotationCols.length),
    autoRefresh: prefs.autoRefresh === undefined ? "unset" : String(prefs.autoRefresh),
    activeJobSets: prefs.activeJobSets === undefined ? "unset" : String(prefs.activeJobSets),
    filterDetails,
  }
}
