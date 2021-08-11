import queryString, { ParseOptions, StringifyOptions } from "query-string"
import { RouteComponentProps } from "react-router-dom"

import { isJobSetsView, JobSetsContainerState } from "../containers/JobSetsContainer"

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
  queryObject.view = state.currentView
  queryObject.newest_first = state.newestFirst
  queryObject.active_only = state.activeOnly

  return queryString.stringify(queryObject)
}

export default class JobSetsQueryStringService {
  routeComponentProps: RouteComponentProps

  constructor(routeComponentProps: RouteComponentProps) {
    this.routeComponentProps = routeComponentProps
  }

  saveState(state: JobSetsContainerState) {
    this.routeComponentProps.history.push({
      ...this.routeComponentProps.location,
      search: makeQueryString(state),
    })
  }

  updateState(state: JobSetsContainerState) {
    const params = queryString.parse(
      this.routeComponentProps.location.search,
      QUERY_STRING_OPTIONS,
    ) as JobSetsQueryParams

    if (params.queue) state.queue = params.queue
    if (params.view && isJobSetsView(params.view)) state.currentView = params.view
    if (params.newest_first != undefined) state.newestFirst = params.newest_first
    if (params.active_only != undefined) state.activeOnly = params.active_only
  }
}
