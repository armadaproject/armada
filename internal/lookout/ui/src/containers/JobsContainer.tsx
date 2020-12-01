import React from 'react'
import * as H from "history"
import { match, withRouter } from 'react-router-dom'
import queryString, { ParseOptions, StringifyOptions } from 'query-string'

import JobService, { JOB_STATES_FOR_DISPLAY, JobInfoViewModel } from "../services/JobService"
import Jobs from "../components/Jobs"
import { updateArray } from "../utils";

type JobsContainerProps = {
  jobService: JobService
  history: H.History;
  location: H.Location;
  match: match;
}

interface JobsContainerState extends JobFilters {
  jobInfos: JobInfoViewModel[]
  canLoadMore: boolean
}

export interface JobFilters {
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
}

type JobFiltersQueryParams = {
  queue?: string
  job_set?: string
  job_states?: string[] | string
  newest_first?: boolean
}

const QUERY_STRING_OPTIONS: ParseOptions | StringifyOptions = {
  arrayFormat: "comma",
  parseBooleans: true,
}

export function makeQueryStringFromFilters(filters: JobFilters): string {
  let queryObject: JobFiltersQueryParams = {}
  if (filters.queue) {
    queryObject = {
      ...queryObject,
      queue: filters.queue,
    }
  }
  if (filters.jobSet) {
    queryObject = {
      ...queryObject,
      job_set: filters.jobSet,
    }
  }
  if (filters.jobStates) {
    queryObject = {
      ...queryObject,
      job_states: filters.jobStates,
    }
  }
  if (filters.newestFirst != null) {
    queryObject = {
      ...queryObject,
      newest_first: filters.newestFirst,
    }
  }

  return queryString.stringify(queryObject, QUERY_STRING_OPTIONS)
}

export function makeFiltersFromQueryString(query: string): JobFilters {
  const params = queryString.parse(query, QUERY_STRING_OPTIONS) as JobFiltersQueryParams

  let filters: JobFilters = {
    queue: "",
    jobSet: "",
    jobStates: [],
    newestFirst: true,
  }
  if (params.queue) {
    filters.queue = params.queue
  }
  if (params.job_set) {
    filters.jobSet = params.job_set
  }
  if (params.job_states) {
    filters.jobStates = parseJobStates(params.job_states)
  }
  if (params.newest_first != null) {
    filters.newestFirst = params.newest_first
  }

  return filters
}

function parseJobStates(jobStates: string[] | string): string[] {
  if (!Array.isArray(jobStates)) {
    if (JOB_STATES_FOR_DISPLAY.includes(jobStates)) {
      return [jobStates]
    } else {
      return []
    }
  }

  return jobStates.filter(jobState => JOB_STATES_FOR_DISPLAY.includes(jobState))
}

class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
  constructor(props: JobsContainerProps) {
    super(props);
    this.state = {
      jobInfos: [],
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
      canLoadMore: true,
    }

    this.loadJobInfos = this.loadJobInfos.bind(this)
    this.jobInfoIsLoaded = this.jobInfoIsLoaded.bind(this)
    this.queueChange = this.queueChange.bind(this)
    this.jobSetChange = this.jobSetChange.bind(this)
    this.jobStatesChange = this.jobStatesChange.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.refresh = this.refresh.bind(this)
    this.setUrlParams = this.setUrlParams.bind(this)
  }

  componentDidMount() {
    const filters = makeFiltersFromQueryString(this.props.location.search)
    this.setState({
      ...this.state,
      ...filters,
    })
  }

  async loadJobInfos(start: number, stop: number): Promise<JobInfoViewModel[]> {
    const take = stop - start;
    const newJobInfos = await this.props.jobService.getJobsInQueue(
      this.state.queue,
      take,
      start,
      [this.state.jobSet],
      this.state.newestFirst,
      this.state.jobStates,
    )

    let canLoadMore = true
    if (take > newJobInfos.length) {
      // No more to be fetched from API
      canLoadMore = false
    }

    updateArray(this.state.jobInfos, newJobInfos, start, stop)
    this.setState({
      ...this.state,
      jobInfos: this.state.jobInfos,
      canLoadMore: canLoadMore
    })
    return this.state.jobInfos
  }

  jobInfoIsLoaded(index: number) {
    return !!this.state.jobInfos[index]
  }

  queueChange(queue: string, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      queue: queue,
    }, () => {
      this.setUrlParams()
      callback()
    })
  }

  jobSetChange(jobSet: string, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      jobSet: jobSet,
    }, () => {
      this.setUrlParams()
      callback()
    })
  }

  jobStatesChange(jobStates: string[], callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      jobStates: jobStates
    }, () => {
      this.setUrlParams()
      callback()
    })
  }

  orderChange(newestFirst: boolean, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      newestFirst: newestFirst
    }, () => {
      this.setUrlParams()
      callback()
    })
  }

  refresh(callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
    }, callback)
  }

  setUrlParams() {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryStringFromFilters(this.state),
    })
  }

  render() {
    return (
      <Jobs
        jobInfos={this.state.jobInfos}
        queue={this.state.queue}
        jobSet={this.state.jobSet}
        jobStates={this.state.jobStates}
        newestFirst={this.state.newestFirst}
        canLoadMore={this.state.canLoadMore}
        fetchJobs={this.loadJobInfos}
        isLoaded={this.jobInfoIsLoaded}
        onQueueChange={this.queueChange}
        onJobSetChange={this.jobSetChange}
        onJobStatesChange={this.jobStatesChange}
        onOrderChange={this.orderChange}
        onRefresh={this.refresh}/>
    )
  }
}

export default withRouter(JobsContainer)
