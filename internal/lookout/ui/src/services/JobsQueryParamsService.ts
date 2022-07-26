import queryString, { ParseOptions, StringifyOptions } from "query-string"
import { RouteComponentProps } from "react-router-dom"

import { ColumnSpec, JobsContainerState } from "../containers/JobsContainer"
import { JOB_STATES_FOR_DISPLAY } from "./JobService"

const QUERY_STRING_OPTIONS: ParseOptions | StringifyOptions = {
  arrayFormat: "comma",
  parseBooleans: true,
}

type JobsQueryParams = {
  queue?: string
  job_set?: string
  job_states?: string[] | string
  newest_first?: boolean
  job_id?: string
  owner?: string
}

export function makeQueryString(columns: ColumnSpec<string | boolean | string[]>[]): string {
  const columnMap = new Map<string, ColumnSpec<string | boolean | string[]>>()
  for (const col of columns) {
    columnMap.set(col.id, col)
  }

  const queueCol = columnMap.get("queue")
  const jobSetCol = columnMap.get("jobSet")
  const jobStateCol = columnMap.get("jobState")
  const submissionTimeCol = columnMap.get("submissionTime")
  const jobIdCol = columnMap.get("jobId")
  const ownerCol = columnMap.get("owner")

  const queryObject: JobsQueryParams = {}
  if (queueCol && queueCol.filter) {
    queryObject.queue = (queueCol.filter as string).trim()
  }
  if (jobSetCol && jobSetCol.filter) {
    queryObject.job_set = (jobSetCol.filter as string).trim()
  }
  if (jobStateCol && jobStateCol.filter) {
    queryObject.job_states = jobStateCol.filter as string[]
  }
  if (submissionTimeCol) {
    queryObject.newest_first = submissionTimeCol.filter as boolean
  }
  if (jobIdCol && jobIdCol.filter) {
    queryObject.job_id = (jobIdCol.filter as string).trim()
  }
  if (ownerCol && ownerCol.filter) {
    queryObject.owner = (ownerCol.filter as string).trim()
  }

  return queryString.stringify(queryObject, QUERY_STRING_OPTIONS)
}

export function updateColumnsFromQueryString(query: string, columns: ColumnSpec<string | boolean | string[]>[]) {
  const params = queryString.parse(query, QUERY_STRING_OPTIONS) as JobsQueryParams
  for (const col of columns) {
    if (col.id === "queue" && params.queue) {
      col.filter = params.queue
    }
    if (col.id === "jobSet" && params.job_set) {
      col.filter = params.job_set
    }
    if (col.id === "jobState" && params.job_states) {
      col.filter = parseJobStates(params.job_states)
    }
    if (col.id === "submissionTime" && params.newest_first != undefined) {
      col.filter = params.newest_first
    }
    if (col.id === "jobId" && params.job_id) {
      col.filter = params.job_id
    }
    if (col.id === "owner" && params.owner) {
      col.filter = params.owner
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
  routeComponentProps: RouteComponentProps

  constructor(routeComponentProps: RouteComponentProps) {
    this.routeComponentProps = routeComponentProps
  }

  saveState(state: JobsContainerState) {
    this.routeComponentProps.history.push({
      ...this.routeComponentProps.location,
      search: makeQueryString(state.defaultColumns),
    })
  }

  updateState(state: JobsContainerState) {
    updateColumnsFromQueryString(this.routeComponentProps.location.search, state.defaultColumns)
  }
}
