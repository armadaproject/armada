import React from 'react'
import * as H from "history"
import { match, withRouter } from 'react-router-dom'
import queryString, { ParseOptions, StringifyOptions } from 'query-string'

import JobService, { JOB_STATES_FOR_DISPLAY, JobInfoViewModel } from "../services/JobService"
import Jobs from "../components/Jobs"

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

const BATCH_SIZE = 100

class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
  constructor(props: JobsContainerProps) {
    super(props)

    const initialFilters = {
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    }
    this.state = {
      ...initialFilters,
      jobInfos: [],
      canLoadMore: true,
    }

    this.serveJobInfos = this.serveJobInfos.bind(this)
    this.jobInfoIsLoaded = this.jobInfoIsLoaded.bind(this)

    this.queueChange = this.queueChange.bind(this)
    this.jobSetChange = this.jobSetChange.bind(this)
    this.jobStatesChange = this.jobStatesChange.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.refresh = this.refresh.bind(this)
  }

  componentDidMount() {
    const filters = makeFiltersFromQueryString(this.props.location.search)
    this.setState({
      ...this.state,
      ...filters,
    })
  }

  async serveJobInfos(start: number, stop: number): Promise<JobInfoViewModel[]> {
    if (start >= this.state.jobInfos.length || stop >= this.state.jobInfos.length) {
      await this.loadJobInfosForRange(start, stop);
    }
    console.log(this.state.jobInfos.length)
    return Promise.resolve(this.state.jobInfos.slice(start, stop))
  }

  jobInfoIsLoaded(index: number) {
    return !!this.state.jobInfos[index]
  }

  async queueChange(queue: string, callback: () => void) {
    const filters = {
      ...this.state,
      queue: queue
    }
    await this.setFilters(filters, callback)
  }

  async jobSetChange(jobSet: string, callback: () => void) {
    const filters = {
      ...this.state,
      jobSet: jobSet
    }
    await this.setFilters(filters, callback)
  }

  async jobStatesChange(jobStates: string[], callback: () => void) {
    const filters = {
      ...this.state,
      jobStates: jobStates
    }
    await this.setFilters(filters, callback)
  }

  async orderChange(newestFirst: boolean, callback: () => void) {
    const filters = {
      ...this.state,
      newestFirst: newestFirst
    }
    await this.setFilters(filters, callback)
  }

  async refresh(callback: () => void) {
    await this.setFilters(this.state, callback)
  }

  private async loadJobInfosForRange(start: number, stop: number) {
    let allJobInfos = this.state.jobInfos
    let canLoadMore = true

    while (start >= allJobInfos.length || stop >= allJobInfos.length) {
      const [newJobInfos, canLoadNext] = await this.fetchNextJobInfos(this.state, this.state.jobInfos.length)
      allJobInfos = allJobInfos.concat(newJobInfos)
      canLoadMore = canLoadNext

      if (!canLoadNext) {
        break
      }
    }

    await this.setStateAsync({
      ...this.state,
      jobInfos: allJobInfos,
      canLoadMore: canLoadMore,
    })
  }

  private async fetchNextJobInfos(filters: JobFilters, startIndex: number): Promise<[JobInfoViewModel[], boolean]> {
    const newJobInfos = await this.props.jobService.getJobsInQueue(
      filters.queue,
      BATCH_SIZE,
      startIndex,
      [filters.jobSet],
      filters.newestFirst,
      filters.jobStates,
    )

    let canLoadMore = true
    if (newJobInfos.length < BATCH_SIZE) {
      canLoadMore = false
    }

    return [newJobInfos, canLoadMore]
  }

  private async setFilters(filters: JobFilters, callback: () => void) {
    await this.setStateAsync({
      ...this.state,
      ...filters,
      jobInfos: [],
      canLoadMore: true,
    })
    this.setUrlParams()
    callback()
  }

  private setStateAsync(state: JobsContainerState): Promise<void> {
    return new Promise(resolve => this.setState(state, resolve))
  }

  private setUrlParams() {
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
        fetchJobs={this.serveJobInfos}
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
