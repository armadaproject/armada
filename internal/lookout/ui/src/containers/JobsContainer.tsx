import React, { Fragment } from "react"

import { RouteComponentProps, withRouter } from "react-router-dom"
import { v4 as uuidv4 } from "uuid"

import JobDetailsModal, { JobDetailsModalContext, toggleExpanded } from "../components/job-details/JobDetailsModal"
import CancelJobsModal, { CancelJobsModalContext, CancelJobsModalState } from "../components/jobs/CancelJobsModal"
import Jobs from "../components/jobs/Jobs"
import ReprioritizeJobsDialog, {
  ReprioritizeJobsDialogContext,
  ReprioritizeJobsDialogState,
} from "../components/jobs/ReprioritizeJobsDialog"
import IntervalService from "../services/IntervalService"
import JobService, { GetJobsRequest, Job } from "../services/JobService"
import JobTableService from "../services/JobTableService"
import JobsLocalStorageService from "../services/JobsLocalStorageService"
import JobsQueryParamsService from "../services/JobsQueryParamsService"
import LogService from "../services/LogService"
import TimerService from "../services/TimerService"
import { RequestStatus, selectItem, setStateAsync } from "../utils"

type JobsContainerProps = {
  jobService: JobService
  logService: LogService
  jobsAutoRefreshMs: number
} & RouteComponentProps

export type JobsContainerState = {
  jobs: Job[]
  selectedJobs: Map<string, Job>
  lastSelectedIndex: number
  autoRefresh: boolean
  defaultColumns: ColumnSpec<string | boolean | string[]>[]
  annotationColumns: ColumnSpec<string>[]
  cancelJobsModalContext: CancelJobsModalContext
  reprioritizeJobsDialogContext: ReprioritizeJobsDialogContext
  jobDetailsModalContext: JobDetailsModalContext
  getJobsRequestStatus: RequestStatus
}

export type ColumnSpec<T> = {
  id: string
  name: string
  accessor: string
  isDisabled: boolean
  filter: T
  defaultFilter: T
}

const newPriorityRegex = new RegExp("^([0-9]+)$")
const BATCH_SIZE = 100
const CANCELLABLE_JOB_STATES = ["Queued", "Pending", "Running"]
const REPRIORITIZEABLE_JOB_STATES = ["Queued", "Pending", "Running"]

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
          isDisabled: false,
          filter: [],
          defaultFilter: [],
        },
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
      ],
      annotationColumns: [],
      cancelJobsModalContext: {
        modalState: "None",
        jobsToCancel: [],
        cancelJobsResult: { cancelledJobs: [], failedJobCancellations: [] },
        cancelJobsRequestStatus: "Idle",
      },
      reprioritizeJobsDialogContext: {
        modalState: "None",
        jobsToReprioritize: [],
        newPriority: 0,
        isValid: false,
        reprioritizeJobsResult: { reprioritizedJobs: [], failedJobReprioritizations: [] },
        reprioritizeJobsRequestStatus: "Idle",
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
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this)
    this.resetAutoRefresh = this.resetAutoRefresh.bind(this)

    this.selectJob = this.selectJob.bind(this)
    this.shiftSelectJob = this.shiftSelectJob.bind(this)
    this.deselectAll = this.deselectAll.bind(this)
    this.setCancelJobsModalState = this.setCancelJobsModalState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)

    this.setReprioritizeJobsDialogState = this.setReprioritizeJobsDialogState.bind(this)
    this.reprioritizeJobs = this.reprioritizeJobs.bind(this)

    this.openJobDetailsModal = this.openJobDetailsModal.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)
    this.closeJobDetailsModal = this.closeJobDetailsModal.bind(this)

    this.addAnnotationColumn = this.addAnnotationColumn.bind(this)
    this.deleteAnnotationColumn = this.deleteAnnotationColumn.bind(this)
    this.changeAnnotationColumnKey = this.changeAnnotationColumnKey.bind(this)

    this.handlePriorityChange = this.handlePriorityChange.bind(this)
    this.registerResetCache = this.registerResetCache.bind(this)

    this.clearFilters = this.clearFilters.bind(this)
  }

  async componentDidMount() {
    const newState = { ...this.state }

    this.localStorageService.updateState(newState)
    this.queryParamsService.updateState(newState)

    this.localStorageService.saveState(newState)
    this.queryParamsService.saveState(newState)

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
      await this.jobTableService.loadJobs(request, start, stop)
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

    this.setFilters(newState)
  }

  addAnnotationColumn() {
    const newState = { ...this.state }
    const newCol = {
      id: uuidv4(),
      name: "",
      accessor: "",
      isDisabled: false,
      filter: "",
      defaultFilter: "",
    }
    newState.annotationColumns.push(newCol)
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
    this.setFilters(newState)
  }

  changeAnnotationColumnKey(columnId: string, newKey: string) {
    const newState = { ...this.state }
    for (const col of newState.annotationColumns) {
      if (col.id === columnId) {
        col.name = newKey
        col.accessor = newKey
      }
    }
    this.setFilters(newState)
  }

  clearFilters() {
    const newState = { ...this.state }
    for (const col of newState.defaultColumns) {
      switch (col.id) {
        case "queue":
        case "jobId":
        case "owner":
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

    const cancellableJobs = this.getCancellableSelectedJobs(selectedJobs)
    const reprioritizeableJobs = this.getReprioritizeableSelectedJobs(selectedJobs)
    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      lastSelectedIndex: index,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: cancellableJobs,
      },
      reprioritizeJobsDialogContext: {
        ...this.state.reprioritizeJobsDialogContext,
        jobsToReprioritize: reprioritizeableJobs,
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
    const reprioritizeableJobs = this.getReprioritizeableSelectedJobs(selectedJobs)
    this.setState({
      ...this.state,
      selectedJobs: selectedJobs,
      lastSelectedIndex: index,
      cancelJobsModalContext: {
        ...this.state.cancelJobsModalContext,
        jobsToCancel: cancellableJobs,
      },
      reprioritizeJobsDialogContext: {
        ...this.state.reprioritizeJobsDialogContext,
        jobsToReprioritize: reprioritizeableJobs,
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
      reprioritizeJobsDialogContext: {
        ...this.state.reprioritizeJobsDialogContext,
        jobsToReprioritize: [],
      },
    })
  }

  setReprioritizeJobsDialogState(modalState: ReprioritizeJobsDialogState) {
    this.setState({
      ...this.state,
      reprioritizeJobsDialogContext: {
        ...this.state.reprioritizeJobsDialogContext,
        modalState: modalState,
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

  async reprioritizeJobs() {
    if (this.state.reprioritizeJobsDialogContext.reprioritizeJobsRequestStatus === "Loading") {
      return
    }

    this.setState({
      ...this.state,
      reprioritizeJobsDialogContext: {
        ...this.state.reprioritizeJobsDialogContext,
        reprioritizeJobsRequestStatus: "Loading",
      },
    })
    const reprioritizeJobsResult = await this.props.jobService.reprioritizeJobs(
      this.state.reprioritizeJobsDialogContext.jobsToReprioritize,
      this.state.reprioritizeJobsDialogContext.newPriority,
    )
    if (reprioritizeJobsResult.failedJobReprioritizations.length === 0) {
      // Succeeded
      this.setState({
        ...this.state,
        jobs: [],
        selectedJobs: new Map<string, Job>(),
        reprioritizeJobsDialogContext: {
          jobsToReprioritize: [],
          isValid: false,
          newPriority: 0,
          reprioritizeJobsResult: reprioritizeJobsResult,
          modalState: "ReprioritizeJobsResult",
          reprioritizeJobsRequestStatus: "Idle",
        },
      })
    } else if (reprioritizeJobsResult.reprioritizedJobs.length === 0) {
      // Failure
      this.setState({
        ...this.state,
        reprioritizeJobsDialogContext: {
          ...this.state.reprioritizeJobsDialogContext,
          reprioritizeJobsResult: reprioritizeJobsResult,
          modalState: "ReprioritizeJobsResult",
          reprioritizeJobsRequestStatus: "Idle",
        },
      })
    } else {
      // Some succeeded, some failed
      this.setState({
        ...this.state,
        jobs: [],
        selectedJobs: new Map<string, Job>(),
        reprioritizeJobsDialogContext: {
          ...this.state.reprioritizeJobsDialogContext,
          jobsToReprioritize: reprioritizeJobsResult.failedJobReprioritizations.map((failed) => failed.job),
          reprioritizeJobsResult: reprioritizeJobsResult,
          modalState: "ReprioritizeJobsResult",
          reprioritizeJobsRequestStatus: "Idle",
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

  async toggleAutoRefresh(autoRefresh: boolean) {
    await setStateAsync(this, {
      ...this.state,
      autoRefresh: autoRefresh,
    })
    this.tryStartAutoRefreshService()
  }

  tryStartAutoRefreshService() {
    if (this.state.autoRefresh) {
      this.autoRefreshService.start()
    } else {
      this.autoRefreshService.stop()
    }
  }

  handlePriorityChange(newValue: string) {
    const valid = newPriorityRegex.test(newValue) && newValue.length > 0
    this.setState({
      ...this.state,
      reprioritizeJobsDialogContext: {
        ...this.state.reprioritizeJobsDialogContext,
        isValid: valid,
        newPriority: Number(newValue),
      },
    })
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

  private async setFilters(updatedState: JobsContainerState) {
    this.localStorageService.saveState(updatedState)
    this.queryParamsService.saveState(updatedState)
    this.jobTableService.refresh()
    await setStateAsync(this, {
      ...updatedState,
      jobs: this.jobTableService.getJobs(),
    })
    this.resetCacheService.start()
  }

  private selectedJobsAreCancellable(): boolean {
    return Array.from(this.state.selectedJobs.values())
      .map((job) => job.jobState)
      .some((jobState) => CANCELLABLE_JOB_STATES.includes(jobState))
  }

  private getCancellableSelectedJobs(selectedJobs: Map<string, Job>): Job[] {
    return Array.from(selectedJobs.values()).filter((job) => CANCELLABLE_JOB_STATES.includes(job.jobState))
  }

  private selectedJobsAreReprioritizeable(): boolean {
    return Array.from(this.state.selectedJobs.values())
      .map((job) => job.jobState)
      .some((jobState) => REPRIORITIZEABLE_JOB_STATES.includes(jobState))
  }

  private getReprioritizeableSelectedJobs(selectedJobs: Map<string, Job>): Job[] {
    return Array.from(selectedJobs.values()).filter((job) => REPRIORITIZEABLE_JOB_STATES.includes(job.jobState))
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
        <ReprioritizeJobsDialog
          modalState={this.state.reprioritizeJobsDialogContext.modalState}
          jobsToReprioritize={this.state.reprioritizeJobsDialogContext.jobsToReprioritize}
          reprioritizeJobsResult={this.state.reprioritizeJobsDialogContext.reprioritizeJobsResult}
          reprioritizeJobsRequestStatus={this.state.reprioritizeJobsDialogContext.reprioritizeJobsRequestStatus}
          isValid={this.state.reprioritizeJobsDialogContext.isValid}
          newPriority={this.state.reprioritizeJobsDialogContext.newPriority}
          onReprioritizeJobs={this.reprioritizeJobs}
          onPriorityChange={this.handlePriorityChange}
          onClose={() => this.setReprioritizeJobsDialogState("None")}
        />
        <JobDetailsModal
          {...this.state.jobDetailsModalContext}
          logService={this.props.logService}
          onToggleExpanded={this.toggleExpanded}
          onClose={this.closeJobDetailsModal}
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
          onCancelJobsClick={() => this.setCancelJobsModalState("CancelJobs")}
          onReprioritizeJobsClick={() => this.setReprioritizeJobsDialogState("ReprioritizeJobs")}
          onJobIdClick={this.openJobDetailsModal}
          onAutoRefreshChange={this.toggleAutoRefresh}
          onInteract={this.resetAutoRefresh}
          onRegisterResetCache={this.registerResetCache}
          onClear={this.clearFilters}
        />
      </Fragment>
    )
  }
}

export default withRouter(JobsContainer)
