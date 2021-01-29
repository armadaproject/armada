import React, { Fragment } from 'react'
import { RouteComponentProps, withRouter } from 'react-router-dom'
import queryString, { ParseOptions, StringifyOptions } from 'query-string'

import JobService, { JobRun, JOB_STATES_FOR_DISPLAY } from "../services/JobService"
import Jobs from "../components/Jobs"
import CancelJobsModal, { CancelJobsModalContext, CancelJobsModalState } from "../components/CancelJobsModal";
import JobDetailsModal, { JobDetailsModalContext, toggleExpanded } from "../components/job-details/JobDetailsModal";

type JobsContainerProps = {
  jobService: JobService
} & RouteComponentProps

export type CancelJobsRequestStatus = "Loading" | "Idle"

interface JobsContainerState extends JobFilters {
  jobs: JobRun[]
  canLoadMore: boolean
  selectedJobs: Map<string, JobRun>
  cancelJobsModalContext: CancelJobsModalContext
  jobDetailsModalContext: JobDetailsModalContext
}

export interface JobFilters {
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  jobId: string
}

type JobFiltersQueryParams = {
  queue?: string
  job_set?: string
  job_states?: string[] | string
  newest_first?: boolean
  job_id?: string
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
  if (filters.jobId) {
    queryObject = {
      ...queryObject,
      job_id: filters.jobId,
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
    jobId: "",
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
  if (params.job_id) {
    filters.jobId = params.job_id
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
      jobId: "",
    }
    this.state = {
      ...initialFilters,
      jobs: [],
      canLoadMore: true,
      selectedJobs: new Map<string, JobRun>(),
      cancelJobsModalContext: {
        modalState: "None",
        jobsToCancel: [],
        cancelJobsResult: { cancelledJobs: [], failedJobCancellations: [] },
        cancelJobsRequestStatus: "Idle",
      },
      jobDetailsModalContext: {
        open: false,
        job: undefined,
        expandedItems: new Set(),
      }
    }

    this.serveJobs = this.serveJobs.bind(this)
    this.jobIsLoaded = this.jobIsLoaded.bind(this)

    this.queueChange = this.queueChange.bind(this)
    this.jobSetChange = this.jobSetChange.bind(this)
    this.jobStatesChange = this.jobStatesChange.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.jobIdChange = this.jobIdChange.bind(this)
    this.refresh = this.refresh.bind(this)

    this.selectJob = this.selectJob.bind(this)
    this.setCancelJobsModalState = this.setCancelJobsModalState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)

    this.openJobDetailsModal = this.openJobDetailsModal.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)
    this.closeJobDetailsModal = this.closeJobDetailsModal.bind(this)
  }

  componentDidMount() {
    const filters = makeFiltersFromQueryString(this.props.location.search)
    this.setState({
      ...this.state,
      ...filters,
    })
  }

  async serveJobs(start: number, stop: number): Promise<JobRun[]> {
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

  async jobIdChange(jobId: string) {
    const filters = {
      ...this.state,
      jobId: jobId,
    }
    await this.setFilters(filters)
  }

  async refresh() {
    await this.setFilters(this.state)
  }

  async selectJob(job: JobRun, selected: boolean) {
    const jobId = job.jobId
    const selectedJobs = new Map<string, JobRun>(this.state.selectedJobs)
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
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: cancellableJobs,
      },
    })
  }

  setCancelJobsModalState(modalState: CancelJobsModalState) {
    this.setState({
      ...this.state,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        modalState: modalState,
      },
    })
  }

  async cancelJobs() {
    if (this.state.cancelJobsModalContext.cancelJobsRequestStatus === "Loading") {
      return
    }

    this.setState({
      ...this.state,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        cancelJobsRequestStatus: "Loading",
      },
    })
    const cancelJobsResult = await this.props.jobService.cancelJobs(this.state.cancelJobsModalContext.jobsToCancel)
    if (cancelJobsResult.failedJobCancellations.length === 0) { // All succeeded
      this.setState({
        ...this.state,
        jobs: [],
        canLoadMore: true,
        selectedJobs: new Map<string, JobRun>(),
        cancelJobsModalContext: {
          jobsToCancel: [],
          cancelJobsResult: cancelJobsResult,
          modalState: "CancelJobsResult",
          cancelJobsRequestStatus: "Idle",
        },
      })
    } else if (cancelJobsResult.cancelledJobs.length === 0) { // All failed
      this.setState({
        ...this.state,
        cancelJobsModalContext: {
          ...this.state.cancelJobsModalContext,
          cancelJobsResult: cancelJobsResult,
          modalState: "CancelJobsResult",
          cancelJobsRequestStatus: "Idle",
        },
      })
    } else { // Some succeeded, some failed
      this.setState({
        ...this.state,
        jobs: [],
        canLoadMore: true,
        selectedJobs: new Map<string, JobRun>(),
        cancelJobsModalContext: {
          ...this.state.cancelJobsModalContext,
          jobsToCancel: cancelJobsResult.failedJobCancellations.map(failed => failed.job),
          cancelJobsResult: cancelJobsResult,
          modalState: "CancelJobsResult",
          cancelJobsRequestStatus: "Idle",
        },
      })
    }
  }

  openJobDetailsModal(jobIndex: number) {
    if (jobIndex < 0 || jobIndex >= this.state.jobs.length) {
      return
    }

    const job = this.state.jobs[jobIndex]
    this.setState({
      ...this.state,
      jobDetailsModalContext: {
        open: true,
        job: job,
        expandedItems: new Set(),
      },
    })
  }

  // Toggle expanded items in scheduling history in Job detail modal
  toggleExpanded(item: string, isExpanded: boolean) {
    const newExpanded = toggleExpanded(item, isExpanded, this.state.jobDetailsModalContext.expandedItems)
    this.setState({
      ...this.state,
      jobDetailsModalContext: {
        ...this.state.jobDetailsModalContext,
        expandedItems: newExpanded,
      }
    })
  }

  closeJobDetailsModal() {
    this.setState({
      ...this.state,
      jobDetailsModalContext: {
        ...this.state.jobDetailsModalContext,
        open: false,
      }
    })
  }

  navigateToJobDetails(jobId: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/job-details",
      search: `id=${jobId}`
    })
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

  private async fetchNextJobInfos(filters: JobFilters, startIndex: number): Promise<[JobRun[], boolean]> {
    const newJobInfos = await this.props.jobService.getJobsInQueue({
      queue: filters.queue,
      take: BATCH_SIZE,
      skip: startIndex,
      jobSets: [filters.jobSet],
      newestFirst: filters.newestFirst,
      jobStates: filters.jobStates,
      jobId: filters.jobId,
    })

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
      selectedJobs: new Map<string, JobRun>(),
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

  private getCancellableSelectedJobs(selectedJobs: Map<string, JobRun>): JobRun[] {
    return Array.from(selectedJobs.values())
      .filter(job => CANCELLABLE_JOB_STATES.includes(job.jobState))
  }

  render() {
    return (
      <Fragment>
        <CancelJobsModal
          modalState={this.state.cancelJobsModalContext.modalState}
          jobsToCancel={this.state.cancelJobsModalContext.jobsToCancel}
          cancelJobsResult={this.state.cancelJobsModalContext.cancelJobsResult}
          cancelJobsRequestStatus={this.state.cancelJobsModalContext.cancelJobsRequestStatus}
          onCancelJobs={this.cancelJobs}
          onClose={() => this.setCancelJobsModalState("None")}/>
        <JobDetailsModal
          open={this.state.jobDetailsModalContext.open}
          job={this.state.jobDetailsModalContext.job}
          expandedItems={this.state.jobDetailsModalContext.expandedItems}
          onToggleExpanded={this.toggleExpanded}
          onClose={this.closeJobDetailsModal} />
        <Jobs
          jobs={this.state.jobs}
          canLoadMore={this.state.canLoadMore}
          queue={this.state.queue}
          jobSet={this.state.jobSet}
          jobStates={this.state.jobStates}
          newestFirst={this.state.newestFirst}
          jobId={this.state.jobId}
          selectedJobs={this.state.selectedJobs}
          cancelJobsButtonIsEnabled={this.selectedJobsAreCancellable()}
          fetchJobs={this.serveJobs}
          isLoaded={this.jobIsLoaded}
          onQueueChange={this.queueChange}
          onJobSetChange={this.jobSetChange}
          onJobStatesChange={this.jobStatesChange}
          onOrderChange={this.orderChange}
          onJobIdChange={this.jobIdChange}
          onRefresh={this.refresh}
          onSelectJob={this.selectJob}
          onCancelJobsClick={() => this.setCancelJobsModalState("CancelJobs")}
          onJobIdClick={this.openJobDetailsModal}/>
      </Fragment>
    )
  }
}

export default withRouter(JobsContainer)
