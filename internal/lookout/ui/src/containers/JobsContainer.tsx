import React from 'react'
import { RouteComponentProps, withRouter } from 'react-router-dom'
import queryString, { ParseOptions, StringifyOptions } from 'query-string'

import JobService, {
  JOB_STATES_FOR_DISPLAY,
  Job,
  CancelJobsResult
} from "../services/JobService"
import Jobs from "../components/Jobs"

type JobsContainerProps = {
  jobService: JobService
} & RouteComponentProps

export type ModalState = "CancelJobs" | "CancelJobsResult" | "None"

export type CancelJobsRequestStatus = "Loading" | "Idle"

interface JobsContainerState extends JobFilters {
  jobs: Job[]
  canLoadMore: boolean
  selectedJobs: Map<string, Job>
  cancellableJobs: Job[]
  modalState: ModalState
  cancelJobsResult: CancelJobsResult
  cancelJobsRequestStatus: CancelJobsRequestStatus
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
const BATCH_SIZE = 100
const CANCELLABLE_JOB_STATES = [
  "Queued",
  "Pending",
  "Running",
]

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
    super(props)

    const initialFilters = {
      queue: "",
      jobSet: "",
      jobStates: [],
      newestFirst: true,
    }
    this.state = {
      ...initialFilters,
      jobs: [],
      canLoadMore: true,
      selectedJobs: new Map<string, Job>(),
      cancellableJobs: [],
      modalState: "None",
      cancelJobsResult: { cancelledJobs: [], failedJobCancellations: [] },
      cancelJobsRequestStatus: "Idle",
    }

    this.serveJobs = this.serveJobs.bind(this)
    this.jobIsLoaded = this.jobIsLoaded.bind(this)

    this.queueChange = this.queueChange.bind(this)
    this.jobSetChange = this.jobSetChange.bind(this)
    this.jobStatesChange = this.jobStatesChange.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.refresh = this.refresh.bind(this)

    this.selectJob = this.selectJob.bind(this)
    this.setModalState = this.setModalState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)
  }

  componentDidMount() {
    const filters = makeFiltersFromQueryString(this.props.location.search)
    this.setState({
      ...this.state,
      ...filters,
    })
  }

  async serveJobs(start: number, stop: number): Promise<Job[]> {
    if (start >= this.state.jobs.length || stop >= this.state.jobs.length) {
      await this.loadJobInfosForRange(start, stop);
    }
    return Promise.resolve(this.state.jobs.slice(start, stop))
  }

  jobIsLoaded(index: number) {
    return !!this.state.jobs[index]
  }

  async queueChange(queue: string) {
    const filters = {
      ...this.state,
      queue: queue
    }
    await this.setFilters(filters)
  }

  async jobSetChange(jobSet: string) {
    const filters = {
      ...this.state,
      jobSet: jobSet
    }
    await this.setFilters(filters)
  }

  async jobStatesChange(jobStates: string[]) {
    const filters = {
      ...this.state,
      jobStates: jobStates
    }
    await this.setFilters(filters)
  }

  async orderChange(newestFirst: boolean) {
    const filters = {
      ...this.state,
      newestFirst: newestFirst
    }
    await this.setFilters(filters)
  }

  async refresh() {
    await this.setFilters(this.state)
  }

  async selectJob(job: Job, selected: boolean) {
    const jobId = job.jobId
    const selectedJobs = new Map<string, Job>(this.state.selectedJobs)
    if (selected) {
      selectedJobs.set(jobId, job)
    } else {
      if (selectedJobs.has(jobId)) {
        selectedJobs.delete(jobId)
      }
    }
    const cancellableJobs = this.getCancellableSelectedJobs(selectedJobs)
    await this.setStateAsync({
      ...this.state,
      selectedJobs: selectedJobs,
      cancellableJobs: cancellableJobs,
    })
  }

  setModalState(modalState: ModalState) {
    this.setState({
      ...this.state,
      modalState: modalState,
    })
  }

  async cancelJobs() {
    if (this.state.cancelJobsRequestStatus === "Loading") {
      return
    }

    this.setState({
      ...this.state,
      cancelJobsRequestStatus: "Loading"
    })
    const cancelJobsResult = await this.props.jobService.cancelJobs(this.state.cancellableJobs)
    if (cancelJobsResult.failedJobCancellations.length === 0) { // All succeeded
      this.setState({
        ...this.state,
        jobs: [],
        canLoadMore: true,
        selectedJobs: new Map<string, Job>(),
        cancellableJobs: [],
        cancelJobsResult: cancelJobsResult,
        modalState: "CancelJobsResult",
        cancelJobsRequestStatus: "Idle",
      })
    } else if (cancelJobsResult.cancelledJobs.length === 0) { // All failed
      this.setState({
        ...this.state,
        cancelJobsResult: cancelJobsResult,
        modalState: "CancelJobsResult",
        cancelJobsRequestStatus: "Idle",
      })
    } else { // Some succeeded, some failed
      this.setState({
        ...this.state,
        jobs: [],
        canLoadMore: true,
        selectedJobs: new Map<string, Job>(),
        cancellableJobs: cancelJobsResult.failedJobCancellations.map(failed => failed.job),
        cancelJobsResult: cancelJobsResult,
        modalState: "CancelJobsResult",
        cancelJobsRequestStatus: "Idle",
      })
    }
  }

  private async loadJobInfosForRange(start: number, stop: number) {
    let allJobInfos = this.state.jobs
    let canLoadMore = true

    while (start >= allJobInfos.length || stop >= allJobInfos.length) {
      const [newJobInfos, canLoadNext] = await this.fetchNextJobInfos(this.state, this.state.jobs.length)
      allJobInfos = allJobInfos.concat(newJobInfos)
      canLoadMore = canLoadNext

      if (!canLoadNext) {
        break
      }
    }

    await this.setStateAsync({
      ...this.state,
      jobs: allJobInfos,
      canLoadMore: canLoadMore,
    })
  }

  private async fetchNextJobInfos(filters: JobFilters, startIndex: number): Promise<[Job[], boolean]> {
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

  private async setFilters(filters: JobFilters) {
    await this.setStateAsync({
      ...this.state,
      ...filters,
      jobs: [],
      canLoadMore: true,
      selectedJobs: new Map<string, Job>(),
    })
    this.setUrlParams()
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

  private selectedJobsAreCancellable(): boolean {
    return Array.from(this.state.selectedJobs.values())
      .map(job => job.jobState)
      .some(jobState => CANCELLABLE_JOB_STATES.includes(jobState))
  }

  private getCancellableSelectedJobs(selectedJobs: Map<string, Job>): Job[] {
    return Array.from(selectedJobs.values())
      .filter(job => CANCELLABLE_JOB_STATES.includes(job.jobState))
  }

  render() {
    return (
      <Jobs
        jobs={this.state.jobs}
        queue={this.state.queue}
        jobSet={this.state.jobSet}
        jobStates={this.state.jobStates}
        newestFirst={this.state.newestFirst}
        canLoadMore={this.state.canLoadMore}
        selectedJobs={this.state.selectedJobs}
        cancelJobsButtonIsEnabled={this.selectedJobsAreCancellable()}
        cancellableJobs={this.state.cancellableJobs}
        cancelJobsResult={this.state.cancelJobsResult}
        modalState={this.state.modalState}
        cancelJobsRequestStatus={this.state.cancelJobsRequestStatus}
        fetchJobs={this.serveJobs}
        isLoaded={this.jobIsLoaded}
        onQueueChange={this.queueChange}
        onJobSetChange={this.jobSetChange}
        onJobStatesChange={this.jobStatesChange}
        onOrderChange={this.orderChange}
        onRefresh={this.refresh}
        onSelectJob={this.selectJob}
        onSetModalState={this.setModalState}
        onCancelJobs={this.cancelJobs} />
    )
  }
}

export default withRouter(JobsContainer)
