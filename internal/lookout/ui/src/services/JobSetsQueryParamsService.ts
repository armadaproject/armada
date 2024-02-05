import queryString, { ParseOptions, StringifyOptions } from "query-string"

import { JobSetsContainerState } from "../containers/JobSetsContainer"
import { Router } from "../utils"

const QUERY_STRING_OPTIONS: ParseOptions | StringifyOptions = {
  arrayFormat: "comma",
  parseBooleans: true,
}

type JobSetsQueryParams = {
  queue?: string
  view?: string
  newest_first?: boolean
  active_only?: boolean
}

export default class JobSetsQueryParamsService {
  constructor(private router: Router) {}

  saveState(state: JobSetsContainerState) {
    const params = queryString.parse(this.router.location.search, QUERY_STRING_OPTIONS) as Record<any, any>

    if (state.queue) {
      params.queue = state.queue
    }
    params.newest_first = state.newestFirst
    params.active_only = state.activeOnly

    this.router.navigate({
      ...this.router.location,
      search: queryString.stringify(params, QUERY_STRING_OPTIONS),
    })
  }

  updateState(state: JobSetsContainerState) {
    const params = queryString.parse(this.router.location.search, QUERY_STRING_OPTIONS) as JobSetsQueryParams

    if (params.queue) state.queue = params.queue
    if (params.newest_first != undefined) state.newestFirst = params.newest_first
    if (params.active_only != undefined) state.activeOnly = params.active_only
  }
}
