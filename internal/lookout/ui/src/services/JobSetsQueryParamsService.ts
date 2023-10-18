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

function makeQueryString(state: JobSetsContainerState): string {
  const queryObject: JobSetsQueryParams = {}

  if (state.queue) {
    queryObject.queue = state.queue
  }
  queryObject.newest_first = state.newestFirst
  queryObject.active_only = state.activeOnly

  return queryString.stringify(queryObject)
}

export default class JobSetsQueryParamsService {
  constructor(private router: Router) {}

  saveState(state: JobSetsContainerState) {
    this.router.navigate({
      ...this.router.location,
      search: makeQueryString(state),
    })
  }

  updateState(state: JobSetsContainerState) {
    const params = queryString.parse(this.router.location.search, QUERY_STRING_OPTIONS) as JobSetsQueryParams

    if (params.queue) state.queue = params.queue
    if (params.newest_first != undefined) state.newestFirst = params.newest_first
    if (params.active_only != undefined) state.activeOnly = params.active_only
  }
}
