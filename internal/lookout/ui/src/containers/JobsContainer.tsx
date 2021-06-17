import React, { Fragment } from "react"

import queryString, { ParseOptions, StringifyOptions } from "query-string"
import { RouteComponentProps, withRouter } from "react-router-dom"
import { v4 as uuidv4 } from "uuid"

import JobDetailsModal, { JobDetailsModalContext, toggleExpanded } from "../components/job-details/JobDetailsModal"
import CancelJobsModal, { CancelJobsModalContext, CancelJobsModalState } from "../components/jobs/CancelJobsModal"
import Jobs from "../components/jobs/Jobs"
import JobService, { GetJobsRequest, JOB_STATES_FOR_DISPLAY, Job } from "../services/JobService"
import { debounced, selectItem } from "../utils"

type JobsContainerProps = {
  jobService: JobService
} & RouteComponentProps

export type CancelJobsRequestStatus = "Loading" | "Idle"

interface JobsContainerState {
  jobs: Job[]
  canLoadMore: boolean
  selectedJobs: Map<string, Job>
  lastSelectedIndex: number
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  cancelJobsModalContext: CancelJobsModalContext
  jobDetailsModalContext: JobDetailsModalContext
}

export type ColumnSpec<T> = {
  id: string
  name: string
  accessor: string
  isDisabled: boolean
  filter: T
  defaultFilter: T
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
const LOCAL_STORAGE_KEY = "armada_lookout_annotation_columns"
const BATCH_SIZE = 100
const CANCELLABLE_JOB_STATES = ["Queued", "Pending", "Running"]

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

  const queryObject: JobFiltersQueryParams = {}
  if (queueCol && queueCol.filter) {
    queryObject.queue = queueCol.filter as string
  }
  if (jobSetCol && jobSetCol.filter) {
    queryObject.job_set = jobSetCol.filter as string
  }
  if (jobStateCol && jobStateCol.filter) {
    queryObject.job_states = jobStateCol.filter as string[]
  }
  if (submissionTimeCol) {
    queryObject.newest_first = submissionTimeCol.filter as boolean
  }
  if (jobIdCol && jobIdCol.filter) {
    queryObject.job_id = jobIdCol.filter as string
  }
  if (ownerCol && ownerCol.filter) {
    queryObject.owner = ownerCol.filter as string
  }

  return queryString.stringify(queryObject, QUERY_STRING_OPTIONS)
}

export function updateColumnsFromQueryString(query: string, columns: ColumnSpec<string | boolean | string[]>[]) {
  const params = queryString.parse(query, QUERY_STRING_OPTIONS) as JobFiltersQueryParams

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
    if (col.id === "submissionTime" && params.newest_first !== undefined) {
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

class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
  constructor(props: JobsContainerProps) {
    super(props)
    this.state = {
      jobs: [],
      canLoadMore: true,
      selectedJobs: new Map<string, Job>(),
      lastSelectedIndex: 0,
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
    }

    this.serveJobs = this.serveJobs.bind(this)
    this.jobIsLoaded = this.jobIsLoaded.bind(this)

    this.changeColumnFilter = this.changeColumnFilter.bind(this)
    this.disableColumn = this.disableColumn.bind(this)
    this.refresh = this.refresh.bind(this)
    this.resetRefresh = this.resetRefresh.bind(this)

    this.selectJob = this.selectJob.bind(this)
    this.shiftSelectJob = this.shiftSelectJob.bind(this)
    this.deselectAll = this.deselectAll.bind(this)
    this.setCancelJobsModalState = this.setCancelJobsModalState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)

    this.openJobDetailsModal = this.openJobDetailsModal.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)
    this.closeJobDetailsModal = this.closeJobDetailsModal.bind(this)

    this.addAnnotationColumn = this.addAnnotationColumn.bind(this)
    this.deleteAnnotationColumn = this.deleteAnnotationColumn.bind(this)
    this.changeAnnotationColumnKey = this.changeAnnotationColumnKey.bind(this)

    this.fetchNextJobInfos = debounced(this.fetchNextJobInfos.bind(this), 100)
  }

  componentDidMount() {
    const annotationColumnsJson = localStorage.getItem(LOCAL_STORAGE_KEY)
    let annotationColumns: ColumnSpec<string>[] | undefined
    if (annotationColumnsJson) {
      annotationColumns = JSON.parse(annotationColumnsJson) as ColumnSpec<string>[]
    }

    updateColumnsFromQueryString(this.props.location.search, this.state.defaultColumns)
    this.setState({
      ...this.state,
      annotationColumns: annotationColumns ?? [],
    })
  }

  async serveJobs(start: number, stop: number): Promise<Job[]> {
    if (start >= this.state.jobs.length || stop >= this.state.jobs.length) {
      await this.loadJobInfosForRange(start, stop)
    }
    return Promise.resolve(this.state.jobs.slice(start, stop))
  }

  jobIsLoaded(index: number) {
    return !!this.state.jobs[index]
  }

  changeColumnFilter(columnId: string, newValue: string | boolean | string[]) {
    for (const col of this.state.defaultColumns) {
      if (col.id === columnId) {
        col.filter = newValue
      }
    }

    for (const col of this.state.annotationColumns) {
      if (col.id === columnId) {
        col.filter = newValue as string
      }
    }

    this.setFilters(this.state)
    this.saveAnnotationColumns()
  }

  disableColumn(columnId: string, isDisabled: boolean) {
    for (const col of this.state.defaultColumns) {
      if (col.id === columnId) {
        col.isDisabled = isDisabled
        col.filter = col.defaultFilter
      }
    }

    for (const col of this.state.annotationColumns) {
      if (col.id === columnId) {
        col.isDisabled = isDisabled
        col.filter = col.defaultFilter
      }
    }

    this.setFilters(this.state)
    this.saveAnnotationColumns()
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
    this.saveAnnotationColumns()
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
    this.saveAnnotationColumns()
  }

  changeAnnotationColumnKey(columnId: string, newKey: string) {
    for (const col of this.state.annotationColumns) {
      if (col.id === columnId) {
        col.name = newKey
        col.accessor = newKey
      }
    }
    this.setFilters(this.state)
    this.saveAnnotationColumns()
  }

  refresh() {
    this.setFilters(this.state)
  }

  resetRefresh() {
    this.setState({
      ...this.state,
    })
  }

  selectJob(index: number, selected: boolean) {
    if (index < 0 || index >= this.state.jobs.length) {
      return
    }
    const job = this.state.jobs[index]

    const selectedJobs = new Map<string, Job>(this.state.selectedJobs)
    selectItem(job.jobId, job, selectedJobs, selected)

    const cancellableJobs = this.getCancellableSelectedJobs(selectedJobs)
    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      lastSelectedIndex: index,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: cancellableJobs,
      },
    })
  }

  shiftSelectJob(index: number, selected: boolean) {
    if (index >= this.state.jobs.length || index < 0) {
      return
    }

    const [start, end] = [this.state.lastSelectedIndex, index].sort((a, b) => a - b)

    const selectedJobs = new Map<string, Job>(this.state.selectedJobs)
    for (let i = start; i <= end; i++) {
      const job = this.state.jobs[i]
      selectItem(job.jobId, job, selectedJobs, selected)
    }

    const cancellableJobs = this.getCancellableSelectedJobs(selectedJobs)
    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      lastSelectedIndex: index,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: cancellableJobs,
      },
    })
  }

  deselectAll() {
    this.setState({
      ...this.state,
      selectedJobs: new Map<string, Job>(),
      lastSelectedIndex: 0,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: [],
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
    if (cancelJobsResult.failedJobCancellations.length === 0) {
      // All succeeded
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
    } else if (cancelJobsResult.cancelledJobs.length === 0) {
      // All failed
      this.setState({
        ...this.state,
        cancelJobsModalContext: {
          ...this.state.cancelJobsModalContext,
          cancelJobsResult: cancelJobsResult,
          modalState: "CancelJobsResult",
          cancelJobsRequestStatus: "Idle",
        },
      })
    } else {
      // Some succeeded, some failed
      this.setState({
        ...this.state,
        jobs: [],
        canLoadMore: true,
        selectedJobs: new Map<string, Job>(),
        cancelJobsModalContext: {
          ...this.state.cancelJobsModalContext,
          jobsToCancel: cancelJobsResult.failedJobCancellations.map((failed) => failed.job),
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
      },
    })
  }

  closeJobDetailsModal() {
    this.setState({
      ...this.state,
      jobDetailsModalContext: {
        ...this.state.jobDetailsModalContext,
        open: false,
      },
    })
  }

  navigateToJobDetails(jobId: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/job-details",
      search: `id=${jobId}`,
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
    const request: GetJobsRequest = {
      queue: "",
      jobId: "",
      owner: "",
      jobSets: [],
      newestFirst: true,
      jobStates: [],
      take: BATCH_SIZE,
      skip: startIndex,
      annotations: {},
    }

    for (const col of this.state.defaultColumns) {
      switch (col.id) {
        case "queue": {
          request.queue = col.filter as string
          break
        }
        case "jobId": {
          request.jobId = col.filter as string
          break
        }
        case "owner": {
          request.owner = col.filter as string
          break
        }
        case "jobSet": {
          request.jobSets = [col.filter as string]
          break
        }
        case "submissionTime": {
          request.newestFirst = col.filter as boolean
          break
        }
        case "jobState": {
          request.jobStates = col.filter as string[]
          break
        }
      }
    }
    for (const col of this.state.annotationColumns) {
      if (col.filter) {
        request.annotations[col.accessor] = col.filter as string
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
    })
    this.setUrlParams()
  }

  private setStateAsync(state: JobsContainerState): Promise<void> {
    return new Promise((resolve) => this.setState(state, resolve))
  }

  private setUrlParams() {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(this.state.defaultColumns),
    })
  }

  private saveAnnotationColumns() {
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(this.state.annotationColumns))
  }

  private selectedJobsAreCancellable(): boolean {
    return Array.from(this.state.selectedJobs.values())
      .map((job) => job.jobState)
      .some((jobState) => CANCELLABLE_JOB_STATES.includes(jobState))
  }

  private getCancellableSelectedJobs(selectedJobs: Map<string, Job>): Job[] {
    return Array.from(selectedJobs.values()).filter((job) => CANCELLABLE_JOB_STATES.includes(job.jobState))
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
          onClose={() => this.setCancelJobsModalState("None")}
        />
        <JobDetailsModal
          open={this.state.jobDetailsModalContext.open}
          job={this.state.jobDetailsModalContext.job}
          expandedItems={this.state.jobDetailsModalContext.expandedItems}
          onToggleExpanded={this.toggleExpanded}
          onClose={this.closeJobDetailsModal}
        />
        <Jobs
          jobs={this.state.jobs}
          canLoadMore={this.state.canLoadMore}
          defaultColumns={this.state.defaultColumns}
          annotationColumns={this.state.annotationColumns}
          selectedJobs={this.state.selectedJobs}
          cancelJobsButtonIsEnabled={this.selectedJobsAreCancellable()}
          fetchJobs={this.serveJobs}
          isLoaded={this.jobIsLoaded}
          onChangeColumnValue={this.changeColumnFilter}
          onDisableColumn={this.disableColumn}
          onDeleteColumn={this.deleteAnnotationColumn}
          onAddColumn={this.addAnnotationColumn}
          onChangeAnnotationColumnKey={this.changeAnnotationColumnKey}
          onRefresh={this.refresh}
          onSelectJob={this.selectJob}
          onShiftSelect={this.shiftSelectJob}
          onDeselectAllClick={this.deselectAll}
          onCancelJobsClick={() => this.setCancelJobsModalState("CancelJobs")}
          onJobIdClick={this.openJobDetailsModal}
          resetRefresh={this.resetRefresh}
        />
      </Fragment>
    )
  }
}

export default withRouter(JobsContainer)
