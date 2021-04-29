import React, { Fragment } from 'react'
import { RouteComponentProps, withRouter } from 'react-router-dom'
import queryString, { ParseOptions, StringifyOptions } from 'query-string'
import { v4 as uuidv4 } from 'uuid';

import JobService, { GetJobsRequest, Job, JOB_STATES_FOR_DISPLAY } from "../services/JobService"
import Jobs from "../components/jobs/Jobs"
import CancelJobsModal, { CancelJobsModalContext, CancelJobsModalState } from "../components/jobs/CancelJobsModal";
import JobDetailsModal, { JobDetailsModalContext, toggleExpanded } from "../components/job-details/JobDetailsModal";
import { debounced } from "../utils";

type JobsContainerProps = {
  jobService: JobService
} & RouteComponentProps

export type CancelJobsRequestStatus = "Loading" | "Idle"

interface JobsContainerState {
  jobs: Job[]
  canLoadMore: boolean
  selectedJobs: Map<string, Job>
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  cancelJobsModalContext: CancelJobsModalContext
  jobDetailsModalContext: JobDetailsModalContext
  forceRefresh: boolean
}

export type ColumnSpec<T> = {
  id: string
  name: string
  accessor: string
  isDisabled: boolean
  filter: T
  defaultFilter: T
}

export interface JobFilters {
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  jobId: string
  owner: string
}

type JobFiltersQueryParams = {
  queue?: string
  job_set?: string
  job_states?: string[] | string
  newest_first?: boolean
  job_id?: string
  owner?: string
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

const DEFAULT_COLUMNS = [
  {
    id: "queue",
    name: "Queue",
  },
  {
    id: "jobId",
    name: "Job Id",
  },
  {
    id: "owner",
    name: "Owner",
  },
  {
    id: "jobSet",
    name: "Job Set",
  },
  {
    id: "submissionTime",
    name: "Submission Time",
  },
  {
    id: "jobState",
    name: "Job State",
  },
]

export function makeQueryStringFromFilters(filters: JobFilters): string {
  let queryObject: JobFiltersQueryParams = {}
  if (filters.queue) {
    queryObject.queue = filters.queue
  }
  if (filters.jobSet) {
    queryObject.job_set = filters.jobSet
  }
  if (filters.jobStates) {
    queryObject.job_states = filters.jobStates
  }
  if (filters.newestFirst != null) {
    queryObject.newest_first = filters.newestFirst
  }
  if (filters.jobId) {
    queryObject.job_id = filters.jobId
  }
  if (filters.owner) {
    queryObject.owner = filters.owner
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
    owner: "",
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
  if (params.owner) {
    filters.owner = params.owner
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
      owner: "",
    }
    this.state = {
      jobs: [],
      canLoadMore: true,
      selectedJobs: new Map<string, Job>(),
      defaultColumns: [
        {
          id: "queue",
          name: "Queue",
          accessor: "queue",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
        },
        {
          id: "jobId",
          name: "Job Id",
          accessor: "jobId",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
        },
        {
          id: "owner",
          name: "Owner",
          accessor: "owner",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
        },
        {
          id: "jobSet",
          name: "Job Set",
          accessor: "jobSet",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
        },
        {
          id: "submissionTime",
          name: "Submission Time",
          accessor: "submissionTime",
          isDisabled: false,
          filter: true,
          defaultFilter: true,
        },
        {
          id: "jobState",
          name: "State",
          accessor: "jobState",
          isDisabled: false,
          filter: [],
          defaultFilter: [],
        },
      ],
      annotationColumns: [],
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
      },
      forceRefresh: false,
    }

    this.serveJobs = this.serveJobs.bind(this)
    this.jobIsLoaded = this.jobIsLoaded.bind(this)

    this.changeColumnFilter = this.changeColumnFilter.bind(this)
    this.disableColumn = this.disableColumn.bind(this)
    this.refresh = this.refresh.bind(this)
    this.resetRefresh = this.resetRefresh.bind(this)

    this.selectJob = this.selectJob.bind(this)
    this.setCancelJobsModalState = this.setCancelJobsModalState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)

    this.openJobDetailsModal = this.openJobDetailsModal.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)
    this.closeJobDetailsModal = this.closeJobDetailsModal.bind(this)

    this.addAnnotationColumn = this.addAnnotationColumn.bind(this)
    this.deleteAnnotationColumn = this.deleteAnnotationColumn.bind(this)
    this.editAnnotationColumnKey = this.editAnnotationColumnKey.bind(this)

    this.fetchNextJobInfos = debounced(this.fetchNextJobInfos.bind(this), 100)
  }

  componentDidMount() {
    /*
    const filters = makeFiltersFromQueryString(this.props.location.search)
    this.setState({
      ...this.state,
      ...filters,
    })
     */
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

  changeColumnFilter(columnId: string, newValue: string | boolean | string[]) {
    for (let col of this.state.defaultColumns) {
      if (col.id === columnId) {
        col.filter = newValue
      }
    }

    for (let col of this.state.annotationColumns) {
      if (col.id === columnId) {
        col.filter = newValue as string
      }
    }

    this.setFilters(this.state)
  }

  disableColumn(columnId: string, isDisabled: boolean) {
    for (let col of this.state.defaultColumns) {
      if (col.id === columnId) {
        col.isDisabled = isDisabled
        col.filter = col.defaultFilter
      }
    }

    for (let col of this.state.annotationColumns) {
      if (col.id === columnId) {
        col.isDisabled = isDisabled
        col.filter = col.defaultFilter
      }
    }

    this.setFilters(this.state)
  }

  addAnnotationColumn() {
    const newCol = {
      id: uuidv4(),
      name: "",
      accessor: "",
      isDisabled: false,
      filter: "",
      defaultFilter: "",
    }
    this.state.annotationColumns.push(newCol)
    this.setFilters(this.state)
    console.log(this.state.annotationColumns)
  }

  deleteAnnotationColumn(columnId: string) {
    let toRemove = -1
    for (let i = 0; i < this.state.annotationColumns.length; i++) {
      if (this.state.annotationColumns[i].id === columnId) {
        toRemove = i
      }
    }

    this.state.annotationColumns.splice(toRemove, 1)
    this.setFilters(this.state)
  }

  editAnnotationColumnKey(columnId: string, newKey: string) {
    console.log("EDIT", columnId, newKey)
    for (let col of this.state.annotationColumns) {
      if (col.id === columnId) {
        col.name = newKey
        col.accessor = newKey
      }
    }
    this.setFilters(this.state)
  }

  refresh() {
    this.setFilters(this.state)
  }

  resetRefresh() {
    this.setState({
      ...this.state,
      forceRefresh: false,
    })
  }

  selectJob(job: Job, selected: boolean) {
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
    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: cancellableJobs,
      },
      forceRefresh: true,
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
        selectedJobs: new Map<string, Job>(),
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
        selectedJobs: new Map<string, Job>(),
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
      const request = this.getJobsRequest(allJobInfos.length)
      const [newJobInfos, canLoadNext] = await this.fetchNextJobInfos(request)
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

  private getJobsRequest(startIndex: number): GetJobsRequest {
    let request: GetJobsRequest = {
      queue: "",
      jobId: "",
      owner: "",
      jobSets: [],
      newestFirst: true,
      jobStates: [],
      take: BATCH_SIZE,
      skip: startIndex,
    }
    for (let col of this.state.defaultColumns) {
      if (col.id === "queue") {
        request.queue = col.filter as string
      } else if (col.id === "jobId") {
        request.jobId = col.filter as string
      } else if (col.id === "owner") {
        request.owner = col.filter as string
      } else if (col.id === "jobSet") {
        request.jobSets = [col.filter as string]
      } else if (col.id === "submissionTime") {
        request.newestFirst = col.filter as boolean
      } else if (col.id === "jobState") {
        request.jobStates = col.filter as string[]
      }
    }

    return request
  }

  private async fetchNextJobInfos(getJobsRequest: GetJobsRequest): Promise<[Job[], boolean]> {
    const newJobInfos = await this.props.jobService.getJobs(getJobsRequest)

    let canLoadMore = true
    if (newJobInfos.length < BATCH_SIZE) {
      canLoadMore = false
    }

    return [newJobInfos, canLoadMore]
  }

  private setFilters(updatedState: JobsContainerState) {
    this.setState({
      ...updatedState,
      jobs: [],
      canLoadMore: true,
      selectedJobs: new Map<string, Job>(),
      forceRefresh: true,
    })
    // this.setUrlParams()
  }

  private setStateAsync(state: JobsContainerState): Promise<void> {
    return new Promise(resolve => this.setState(state, resolve))
  }

  private setUrlParams() {
    this.props.history.push({
      ...this.props.location,
      // search: makeQueryStringFromFilters(this.state),
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
          onClose={this.closeJobDetailsModal}/>
        <Jobs
          jobs={this.state.jobs}
          canLoadMore={this.state.canLoadMore}
          defaultColumns={this.state.defaultColumns}
          annotationColumns={this.state.annotationColumns}
          selectedJobs={this.state.selectedJobs}
          forceRefresh={this.state.forceRefresh}
          cancelJobsButtonIsEnabled={this.selectedJobsAreCancellable()}
          fetchJobs={this.serveJobs}
          isLoaded={this.jobIsLoaded}
          onChangeColumn={this.changeColumnFilter}
          onDisableColumn={this.disableColumn}
          onDeleteColumn={this.deleteAnnotationColumn}
          onAddColumn={this.addAnnotationColumn}
          onEditColumn={this.editAnnotationColumnKey}
          onRefresh={this.refresh}
          onSelectJob={this.selectJob}
          onCancelJobsClick={() => this.setCancelJobsModalState("CancelJobs")}
          onJobIdClick={this.openJobDetailsModal}
          resetRefresh={this.resetRefresh}/>
      </Fragment>
    )
  }
}

export default withRouter(JobsContainer)
