import queryString, { ParseOptions, StringifyOptions } from "query-string"

import { JobSetsContainerState } from "../containers/JobSetsContainer"
import { Router } from "../utils"
import { JobSetsOrderByColumn } from "./JobService"

const QUERY_STRING_OPTIONS: ParseOptions | StringifyOptions = {
  arrayFormat: "comma",
  parseBooleans: true,
}

type JobSetsQueryParams = {
  queue?: string
  view?: string
  active_only?: boolean
  order_by_column?: JobSetsOrderByColumn
  order_by_desc?: boolean
}

export default class JobSetsQueryParamsService {
  constructor(private router: Router) {}

  saveState(state: JobSetsContainerState) {
    const params = queryString.parse(this.router.location.search, QUERY_STRING_OPTIONS) as Record<any, any>

    if (state.queue) {
      params.queue = state.queue
    }
    params.active_only = state.activeOnly
    params.order_by_column = state.orderByColumn
    params.order_by_desc = state.orderByDesc

    this.router.navigate({
      ...this.router.location,
      search: queryString.stringify(params, QUERY_STRING_OPTIONS),
    })
  }

  updateState(state: JobSetsContainerState) {
    const params = queryString.parse(this.router.location.search, QUERY_STRING_OPTIONS) as JobSetsQueryParams

    if (params.queue) state.queue = params.queue
    if (params.order_by_column != undefined) state.orderByColumn = params.order_by_column
    if (params.order_by_desc != undefined) state.orderByDesc = params.order_by_desc
    if (params.active_only != undefined) state.activeOnly = params.active_only
  }
}
