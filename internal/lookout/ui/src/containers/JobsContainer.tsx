import React from "react"

import { v4 as uuidv4 } from "uuid"

import Jobs from "../components/jobs/Jobs"
import IntervalService from "../services/IntervalService"
import { JobService, GetJobsRequest, Job } from "../services/JobService"
import JobTableService from "../services/JobTableService"
import JobsLocalStorageService from "../services/JobsLocalStorageService"
import JobsQueryParamsService from "../services/JobsQueryParamsService"
import LogService from "../services/LogService"
import TimerService from "../services/TimerService"
import { ApiResult, PropsWithRouter, RequestStatus, selectItem, setStateAsync, withRouter } from "../utils"
import CancelJobsDialog from "./CancelJobsDialog"
import JobDialog from "./JobDialog"
import ReprioritizeJobsDialog from "./ReprioritizeJobsDialog"

interface JobsContainerProps extends PropsWithRouter {
  jobService: JobService
  logService: LogService
  jobsAutoRefreshMs: number
}

export type JobsContainerState = {
  jobs: Job[]
  selectedJobs: Map<string, Job>
  lastSelectedIndex: number
  autoRefresh: boolean
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  cancelJobsIsOpen: boolean
  reprioritizeJobsIsOpen: boolean
  jobDialogIsOpen: boolean
  clickedJob?: Job
  getJobsRequestStatus: RequestStatus
  abortController: AbortController
}

export type ColumnSpec<T> = {
  id: string
  name: string
  accessor: string
  urlParamKey: string
  isDisabled: boolean
  filter: T
  defaultFilter: T
  width: number // Relative weight of column w.r.t. other columns, default is 1
}

export function isColumnSpec<T>(obj: any): obj is ColumnSpec<T> {
  if (obj == undefined || typeof obj != "object") {
    return false
  }

  const columnSpec = obj as Record<string, unknown>
  return (
    columnSpec.id !== undefined &&
    typeof columnSpec.id === "string" &&
    columnSpec.name !== undefined &&
    typeof columnSpec.name === "string" &&
    columnSpec.accessor !== undefined &&
    typeof columnSpec.accessor === "string" &&
    columnSpec.urlParamKey !== undefined &&
    typeof columnSpec.urlParamKey === "string" &&
    columnSpec.isDisabled !== undefined &&
    typeof columnSpec.isDisabled === "boolean" &&
    columnSpec.filter !== undefined &&
    columnSpec.defaultFilter !== undefined &&
    columnSpec.width !== undefined &&
    typeof columnSpec.width === "number"
  )
}

const BATCH_SIZE = 100
export const CANCELLABLE_JOB_STATES = ["Queued", "Pending", "Running"]
export const REPRIORITIZEABLE_JOB_STATES = ["Queued", "Pending", "Running"]

class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
  jobTableService: JobTableService
  autoRefreshService: IntervalService
  resetCacheService: TimerService
  localStorageService: JobsLocalStorageService
  queryParamsService: JobsQueryParamsService

  constructor(props: JobsContainerProps) {
    super(props)

    this.jobTableService = new JobTableService(this.props.jobService, BATCH_SIZE)
    this.autoRefreshService = new IntervalService(props.jobsAutoRefreshMs)
    this.resetCacheService = new TimerService(100)
    this.localStorageService = new JobsLocalStorageService()
    this.queryParamsService = new JobsQueryParamsService(this.props)

    this.state = {
      jobs: [],
      getJobsRequestStatus: "Idle",
      selectedJobs: new Map<string, Job>(),
      lastSelectedIndex: 0,
      autoRefresh: true,
      defaultColumns: [
        {
          id: "jobState",
          name: "State",
          accessor: "jobState",
          urlParamKey: "job_states",
          isDisabled: false,
          filter: [],
          defaultFilter: [],
          width: 0.5,
        },
        {
          id: "jobStateDuration",
          name: "Time in State",
          accessor: "jobStateDuration",
          urlParamKey: "job_state_duration",
          isDisabled: false,
          filter: [],
          defaultFilter: [],
          width: 0.5,
        },
        {
          id: "queue",
          name: "Queue",
          accessor: "queue",
          urlParamKey: "queue",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
          width: 1,
        },
        {
          id: "jobId",
          name: "Job Id",
          accessor: "jobId",
          urlParamKey: "job_id",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
          width: 1,
        },
        {
          id: "owner",
          name: "Owner",
          accessor: "owner",
          urlParamKey: "owner",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
          width: 1,
        },
        {
          id: "jobSet",
          name: "Job Set",
          accessor: "jobSet",
          urlParamKey: "job_set",
          isDisabled: false,
          filter: "",
          defaultFilter: "",
          width: 1,
        },
        {
          id: "submissionTime",
          name: "Submission Time",
          accessor: "submissionTime",
          urlParamKey: "newest_first",
          isDisabled: false,
          filter: true,
          defaultFilter: true,
          width: 1,
        },
      ],
      annotationColumns: [],
      cancelJobsIsOpen: false,
      reprioritizeJobsIsOpen: false,
      jobDialogIsOpen: false,
      abortController: new AbortController(),
    }

    this.serveJobs = this.serveJobs.bind(this)
    this.jobIsLoaded = this.jobIsLoaded.bind(this)

    this.changeColumnFilter = this.changeColumnFilter.bind(this)
    this.disableColumn = this.disableColumn.bind(this)
    this.refresh = this.refresh.bind(this)
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this)
    this.resetAutoRefresh = this.resetAutoRefresh.bind(this)

    this.selectJob = this.selectJob.bind(this)
    this.shiftSelectJob = this.shiftSelectJob.bind(this)
    this.deselectAll = this.deselectAll.bind(this)

    this.openJobDetails = this.openJobDetails.bind(this)
    this.jobClicked = this.jobClicked.bind(this)

    this.openCancelJobs = this.openCancelJobs.bind(this)
    this.openReprioritizeJobs = this.openReprioritizeJobs.bind(this)
    this.handleApiResult = this.handleApiResult.bind(this)

    this.addAnnotationColumn = this.addAnnotationColumn.bind(this)
    this.deleteAnnotationColumn = this.deleteAnnotationColumn.bind(this)
    this.changeAnnotationColumnKey = this.changeAnnotationColumnKey.bind(this)

    this.registerResetCache = this.registerResetCache.bind(this)

    this.clearFilters = this.clearFilters.bind(this)
  }

  async componentDidMount() {
    const newState = { ...this.state }

    this.localStorageService.updateState(newState)
    this.queryParamsService.updateState(newState)

    await setStateAsync(this, {
      ...newState,
      jobs: this.jobTableService.getJobs(), // Can start loading
    })

    this.autoRefreshService.registerCallback(this.refresh)
    this.tryStartAutoRefreshService()
  }

  componentWillUnmount() {
    this.resetCacheService.stop()
    this.autoRefreshService.stop()
  }

  async serveJobs(start: number, stop: number): Promise<Job[]> {
    if (this.state.getJobsRequestStatus === "Loading") {
      return Promise.resolve([])
    }

    await setStateAsync(this, {
      ...this.state,
      getJobsRequestStatus: "Loading",
    })

    let shouldLoad = false
    for (let i = start; i <= stop; i++) {
      if (!this.jobTableService.jobIsLoaded(i)) {
        shouldLoad = true
        break
      }
    }

    if (shouldLoad) {
      const request = this.createGetJobsRequest()
      await this.jobTableService.loadJobs(request, start, stop, this.state.abortController.signal)
      await setStateAsync(this, {
        ...this.state,
        jobs: this.jobTableService.getJobs(),
        getJobsRequestStatus: "Idle",
      })
    }

    return Promise.resolve(this.state.jobs.slice(start, stop))
  }

  jobIsLoaded(index: number) {
    return this.state.getJobsRequestStatus === "Loading" || this.jobTableService.jobIsLoaded(index)
  }

  changeColumnFilter(columnId: string, newValue: string | boolean | string[]) {
    const newState = { ...this.state }

    for (const col of newState.defaultColumns) {
      if (col.id === columnId) {
        col.filter = newValue
      }
    }

    for (const col of newState.annotationColumns) {
      if (col.id === columnId) {
        col.filter = newValue as string
      }
    }

    this.setFilters(newState)
  }

  disableColumn(columnId: string, isDisabled: boolean) {
    const newState = { ...this.state }

    for (const col of newState.defaultColumns) {
      if (col.id === columnId) {
        col.isDisabled = isDisabled
        col.filter = col.defaultFilter
      }
    }

    for (const col of newState.annotationColumns) {
      if (col.id === columnId) {
        col.isDisabled = isDisabled
        col.filter = col.defaultFilter
      }
    }

    this.localStorageService.saveState(newState)
    this.setFilters(newState)
  }

  addAnnotationColumn() {
    const newState = { ...this.state }
    const newCol: ColumnSpec<string> = {
      id: uuidv4(),
      name: "",
      accessor: "",
      urlParamKey: "",
      isDisabled: false,
      filter: "",
      defaultFilter: "",
      width: 1,
    }
    newState.annotationColumns.push(newCol)
    this.localStorageService.saveState(newState)
    this.setFilters(newState)
  }

  deleteAnnotationColumn(columnId: string) {
    const newState = { ...this.state }
    let toRemove = -1
    for (let i = 0; i < newState.annotationColumns.length; i++) {
      if (newState.annotationColumns[i].id === columnId) {
        toRemove = i
      }
    }

    newState.annotationColumns.splice(toRemove, 1)
    this.localStorageService.saveState(newState)
    this.setFilters(newState)
  }

  changeAnnotationColumnKey(columnId: string, newKey: string) {
    const newState = { ...this.state }
    for (const col of newState.annotationColumns) {
      if (col.id === columnId) {
        col.name = newKey
        col.accessor = newKey
        col.urlParamKey = newKey
      }
    }
    this.localStorageService.saveState(newState)
    this.setFilters(newState)
  }

  clearFilters() {
    const newState = { ...this.state }
    for (const col of newState.defaultColumns) {
      switch (col.id) {
        case "queue":
        case "jobId":
        case "owner":
        case "jobStateDuration":
        case "jobSet": {
          col.filter = ""
          break
        }
        case "submissionTime": {
          col.filter = true
          break
        }
        case "jobState": {
          col.filter = []
          break
        }
      }
    }

    for (const col of newState.annotationColumns) {
      col.filter = ""
    }

    this.setFilters(newState)
  }

  refresh() {
    this.setFilters(this.state)
  }

  resetAutoRefresh() {
    this.autoRefreshService.start()
  }

  selectJob(index: number, selected: boolean) {
    if (index < 0 || index >= this.state.jobs.length) {
      return
    }
    const job = this.state.jobs[index]

    const selectedJobs = new Map<string, Job>(this.state.selectedJobs)
    selectItem(job.jobId, job, selectedJobs, selected)

    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      lastSelectedIndex: index,
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

    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      lastSelectedIndex: index,
    })
  }

  deselectAll() {
    this.setState({
      ...this.state,
      selectedJobs: new Map<string, Job>(),
      lastSelectedIndex: 0,
    })
  }

  openJobDetails(isOpen: boolean) {
    this.setState({
      ...this.state,
      jobDialogIsOpen: isOpen,
    })
  }

  getJobByIndex(jobIndex: number): Job | null {
    if (jobIndex < 0 || jobIndex >= this.state.jobs.length) {
      return null
    }
    return this.state.jobs[jobIndex]
  }

  jobClicked(jobIndex: number) {
    const job = this.getJobByIndex(jobIndex)
    if (job) {
      this.setState({
        ...this.state,
        clickedJob: job,
        jobDialogIsOpen: true,
      })
    }
  }

  openCancelJobs(isOpen: boolean) {
    this.setState({
      ...this.state,
      cancelJobsIsOpen: isOpen,
    })
  }

  openReprioritizeJobs(isOpen: boolean) {
    this.setState({
      ...this.state,
      reprioritizeJobsIsOpen: isOpen,
    })
  }

  async handleApiResult(result: ApiResult) {
    if (result === "Success") {
      this.deselectAll()
      this.refresh()
    } else if (result === "Partial success") {
      this.refresh()
    }
  }

  async toggleAutoRefresh(autoRefresh: boolean) {
    const newState = {
      ...this.state,
      autoRefresh: autoRefresh,
    }
    await setStateAsync(this, newState)
    this.localStorageService.saveState(newState)
    this.tryStartAutoRefreshService()
  }

  tryStartAutoRefreshService() {
    if (this.state.autoRefresh) {
      this.autoRefreshService.start()
    } else {
      this.autoRefreshService.stop()
    }
  }

  registerResetCache(resetCache: () => void) {
    this.resetCacheService.registerCallback(resetCache)
  }

  private createGetJobsRequest(): GetJobsRequest {
    const request: GetJobsRequest = {
      queue: "",
      jobId: "",
      owner: "",
      jobSets: [],
      newestFirst: true,
      jobStates: [],
      take: BATCH_SIZE,
      skip: 0,
      annotations: {},
    }

    for (const col of this.state.defaultColumns) {
      switch (col.id) {
        // Queue, jobId, and owner are user input and if is possible to have user put in whitespace.
        case "queue": {
          request.queue = (col.filter as string).trim()
          break
        }
        case "jobId": {
          request.jobId = (col.filter as string).trim()
          break
        }
        case "owner": {
          request.owner = (col.filter as string).trim()
          break
        }
        case "jobSet": {
          request.jobSets = [(col.filter as string).trim()]
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

  private async setFilters(updatedState: JobsContainerState) {
    this.state.abortController.abort()
    this.queryParamsService.saveState(updatedState)
    this.jobTableService.refresh()
    await setStateAsync(this, {
      ...updatedState,
      jobs: this.jobTableService.getJobs(),
      abortController: new AbortController(),
    })
    this.resetCacheService.start()
  }

  private selectedJobsAreCancellable(): boolean {
    return Array.from(this.state.selectedJobs.values())
      .map((job) => job.jobState)
      .some((jobState) => CANCELLABLE_JOB_STATES.includes(jobState))
  }

  private selectedJobsAreReprioritizeable(): boolean {
    return Array.from(this.state.selectedJobs.values())
      .map((job) => job.jobState)
      .some((jobState) => REPRIORITIZEABLE_JOB_STATES.includes(jobState))
  }

  render() {
    const selectedJobs = Array.from(this.state.selectedJobs.values())
    return (
      <>
        <CancelJobsDialog
          isOpen={this.state.cancelJobsIsOpen}
          selectedJobs={selectedJobs}
          jobService={this.props.jobService}
          onResult={this.handleApiResult}
          onClose={() => this.openCancelJobs(false)}
        />
        <ReprioritizeJobsDialog
          isOpen={this.state.reprioritizeJobsIsOpen}
          selectedJobs={selectedJobs}
          jobService={this.props.jobService}
          onResult={this.handleApiResult}
          onClose={() => this.openReprioritizeJobs(false)}
        />
        <JobDialog
          isOpen={this.state.jobDialogIsOpen}
          job={this.state.clickedJob}
          logService={this.props.logService}
          onClose={() => this.openJobDetails(false)}
        />
        <Jobs
          jobs={this.state.jobs}
          defaultColumns={this.state.defaultColumns}
          annotationColumns={this.state.annotationColumns}
          selectedJobs={this.state.selectedJobs}
          autoRefresh={this.state.autoRefresh}
          getJobsRequestStatus={this.state.getJobsRequestStatus}
          cancelJobsButtonIsEnabled={this.selectedJobsAreCancellable()}
          reprioritizeButtonIsEnabled={this.selectedJobsAreReprioritizeable()}
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
          onCancelJobsClick={() => this.openCancelJobs(true)}
          onReprioritizeJobsClick={() => this.openReprioritizeJobs(true)}
          onJobIdClick={this.jobClicked}
          onAutoRefreshChange={this.toggleAutoRefresh}
          onInteract={this.resetAutoRefresh}
          onRegisterResetCache={this.registerResetCache}
          onClear={this.clearFilters}
        />
      </>
    )
  }
}

export default withRouter((props: JobsContainerProps) => <JobsContainer {...props} />)
