import queryString, { ParseOptions, StringifiableRecord, StringifyOptions } from "query-string"

import { JOB_STATES_FOR_DISPLAY } from "./JobService"
import { ColumnSpec, JobsContainerState } from "../containers/JobsContainer"
import { PropsWithRouter, Router } from "../utils"

const QUERY_STRING_OPTIONS: ParseOptions | StringifyOptions = {
  arrayFormat: "comma",
  parseBooleans: true,
}

export function makeQueryString(
  defaultColumns: ColumnSpec<string | boolean | string[]>[],
  annotationColumns: ColumnSpec<string>[],
): string {
  const record: StringifiableRecord = {}

  for (const defaultCol of defaultColumns) {
    if (defaultCol.filter !== undefined) {
      if (typeof defaultCol.filter === "string" && defaultCol.filter) {
        record[defaultCol.urlParamKey] = (defaultCol.filter as string).trim()
      } else if (typeof defaultCol.filter === "boolean" || defaultCol.filter) {
        record[defaultCol.urlParamKey] = defaultCol.filter
      }
    }
  }

  for (const annotationCol of annotationColumns) {
    if (annotationCol.urlParamKey && annotationCol.filter) {
      record[annotationCol.urlParamKey] = annotationCol.filter.trim()
    }
  }

  return queryString.stringify(record, QUERY_STRING_OPTIONS)
}

export function updateColumnsFromQueryString(
  query: string,
  defaultColumns: ColumnSpec<string | boolean | string[]>[],
  annotationColumns: ColumnSpec<string>[],
) {
  const columnMap = new Map<string, ColumnSpec<string | boolean | string[]>>()
  for (const col of defaultColumns) {
    columnMap.set(col.urlParamKey, col)
  }
  for (const col of annotationColumns) {
    columnMap.set(col.urlParamKey, col)
  }

  const record: StringifiableRecord = queryString.parse(query, QUERY_STRING_OPTIONS)
  for (const [key, filter] of Object.entries(record)) {
    const col = columnMap.get(key)
    if (col) {
      if (key === "job_states") {
        col.filter = parseJobStates(filter as string | string[])
      } else {
        col.filter = filter as string | boolean | string[]
      }
    }
  }
}

function parseJobStates(jobStates: string[] | string): string[] {
  if (!Array.isArray(jobStates)) {
    if (JOB_STATES_FOR_DISPLAY.includes(jobStates)) {
      return [jobStates]
    } else {
      return []
    }
  }

  return jobStates.filter((jobState) => JOB_STATES_FOR_DISPLAY.includes(jobState))
}

export default class JobsQueryParamsService {
  router: Router

  constructor(routeComponentProps: PropsWithRouter) {
    this.router = routeComponentProps.router
  }

  saveState(state: JobsContainerState) {
    this.router.navigate({
      ...this.router.location,
      search: makeQueryString(state.defaultColumns, state.annotationColumns),
    })
  }

  updateState(state: JobsContainerState) {
    updateColumnsFromQueryString(this.router.location.search, state.defaultColumns, state.annotationColumns)
  }
}
