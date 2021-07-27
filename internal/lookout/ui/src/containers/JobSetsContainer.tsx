import * as queryString from "querystring"

import React, { Fragment } from "react"

import { RouteComponentProps, withRouter } from "react-router-dom"

import CancelJobSetsDialog, {
  CancelJobSetsDialogContext,
  CancelJobSetsDialogState,
} from "../components/job-sets/CancelJobSetsDialog"
import JobSets from "../components/job-sets/JobSets"
import ReprioritizeJobSetsDialog, {
  ReprioritizeJobSetsDialogContext,
  ReprioritizeJobSetsDialogState,
} from "../components/job-sets/ReprioritizeJobSetsDialog"
import IntervalService from "../services/IntervalService"
import JobService, { GetJobSetsRequest, JobSet } from "../services/JobService"
import { setStateAsync, selectItem, debounced } from "../utils"
import { RequestStatus } from "./JobsContainer"

type JobSetsContainerProps = {
  jobService: JobService
  jobSetsAutoRefreshMs: number
} & RouteComponentProps

type JobSetsContainerParams = {
  queue: string
  currentView: JobSetsView
}

const newPriorityRegex = new RegExp("^([0-9]+)$")

type JobSetsContainerState = {
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  getJobSetsRequestStatus: RequestStatus
  autoRefresh: boolean
  lastSelectedIndex: number
  newestFirst: boolean
  activeOnly: boolean
  cancelJobSetsDialogContext: CancelJobSetsDialogContext
  reprioritizeJobSetsDialogContext: ReprioritizeJobSetsDialogContext
} & JobSetsContainerParams

export type JobSetsView = "job-counts" | "runtime" | "queued-time"

type JobSetsQueryParams = {
  queue?: string
  view?: string
}

export function isJobSetsView(val: string): val is JobSetsView {
  return ["job-counts", "runtime", "queued-time"].includes(val)
}

function makeQueryString(queue: string, view: JobSetsView): string {
  const queryObject: JobSetsQueryParams = {}

  if (queue) {
    queryObject.queue = queue
  }
  queryObject.view = view

  return queryString.stringify(queryObject)
}

function getParamsFromQueryString(query: string): JobSetsContainerParams {
  if (query[0] === "?") {
    query = query.slice(1)
  }
  const params = queryString.parse(query) as JobSetsQueryParams

  return {
    queue: params.queue ?? "",
    currentView: params.view && isJobSetsView(params.view) ? params.view : "job-counts",
  }
}

class JobSetsContainer extends React.Component<JobSetsContainerProps, JobSetsContainerState> {
  autoRefreshService: IntervalService

  constructor(props: JobSetsContainerProps) {
    super(props)

    this.autoRefreshService = new IntervalService(props.jobSetsAutoRefreshMs)

    this.state = {
      queue: "",
      jobSets: [],
      currentView: "job-counts",
      selectedJobSets: new Map<string, JobSet>(),
      getJobSetsRequestStatus: "Idle",
      autoRefresh: true,
      lastSelectedIndex: 0,
      cancelJobSetsDialogContext: {
        dialogState: "None",
        queue: "",
        jobSetsToCancel: [],
        cancelJobSetsResult: { cancelledJobSets: [], failedJobSetCancellations: [] },
        cancelJobSetsRequestStatus: "Idle",
      },
      reprioritizeJobSetsDialogContext: {
        dialogState: "None",
        queue: "",
        isValid: false,
        newPriority: 0,
        jobSetsToReprioritize: [],
        reprioritizeJobSetsResult: { reprioritizedJobSets: [], failedJobSetReprioritizations: [] },
        reproiritizeJobSetsRequestStatus: "Idle",
      },
      newestFirst: true,
      activeOnly: false,
    }

    this.setQueue = this.setQueue.bind(this)
    this.setView = this.setView.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.activeOnlyChange = this.activeOnlyChange.bind(this)
    this.navigateToJobSetForState = this.navigateToJobSetForState.bind(this)
    this.selectJobSet = this.selectJobSet.bind(this)
    this.shiftSelectJobSet = this.shiftSelectJobSet.bind(this)
    this.deselectAll = this.deselectAll.bind(this)
    this.setCancelJobSetsDialogState = this.setCancelJobSetsDialogState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)
    this.reprioritizeJobSets = this.reprioritizeJobSets.bind(this)
    this.handlePriorityChange = this.handlePriorityChange.bind(this)

    this.fetchJobSets = debounced(this.fetchJobSets.bind(this), 100)
    this.loadJobSets = this.loadJobSets.bind(this)
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this)
  }

  async componentDidMount() {
    const params = getParamsFromQueryString(this.props.location.search)
    await this.setStateAsync({
      ...this.state,
      ...params,
    })

    await this.loadJobSets()

    this.autoRefreshService.registerCallback(this.loadJobSets)
    this.autoRefreshService.start()
  }

  componentWillUnmount() {
    this.autoRefreshService.stop()
  }

  async setQueue(queue: string) {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(queue, this.state.currentView),
    })
    await this.setStateAsync({
      ...this.state,
      queue: queue,
    })

    // Performed separately because debounced
    await this.loadJobSets()
  }

  async orderChange(newestFirst: boolean) {
    await this.setStateAsync({
      ...this.state,
      newestFirst: newestFirst,
    })

    await this.loadJobSets()
  }

  async activeOnlyChange(activeOnly: boolean) {
    await this.setStateAsync({
      ...this.state,
      activeOnly: activeOnly,
    })

    await this.loadJobSets()
  }

  selectJobSet(index: number, selected: boolean) {
    if (index < 0 || index >= this.state.jobSets.length) {
      return
    }
    const jobSet = this.state.jobSets[index]

    const selectedJobSets = new Map<string, JobSet>(this.state.selectedJobSets)
    selectItem(jobSet.jobSetId, jobSet, selectedJobSets, selected)

    const cancellableJobSets = JobSetsContainer.getCancellableSelectedJobSets(selectedJobSets)
    const reprioritizeableJobSets = JobSetsContainer.getReprioritizeableJobSets(selectedJobSets)
    this.setState({
      ...this.state,
      selectedJobSets: selectedJobSets,
      lastSelectedIndex: index,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: cancellableJobSets,
      },
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        jobSetsToReprioritize: reprioritizeableJobSets,
      },
    })
  }

  shiftSelectJobSet(index: number, selected: boolean) {
    if (index >= this.state.jobSets.length || index < 0) {
      return
    }

    const [start, end] = [this.state.lastSelectedIndex, index].sort((a, b) => a - b)

    const selectedJobSets = new Map<string, JobSet>(this.state.selectedJobSets)
    for (let i = start; i <= end; i++) {
      const jobSet = this.state.jobSets[i]
      selectItem(jobSet.jobSetId, jobSet, selectedJobSets, selected)
    }

    const cancellableJobSets = JobSetsContainer.getCancellableSelectedJobSets(selectedJobSets)
    const reprioritizeableJobSets = JobSetsContainer.getReprioritizeableJobSets(selectedJobSets)
    this.setState({
      ...this.state,
      selectedJobSets: selectedJobSets,
      lastSelectedIndex: index,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: cancellableJobSets,
      },
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        jobSetsToReprioritize: reprioritizeableJobSets,
      },
    })
  }

  deselectAll() {
    this.setState({
      ...this.state,
      selectedJobSets: new Map<string, JobSet>(),
      lastSelectedIndex: 0,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: [],
      },
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        jobSetsToReprioritize: [],
      },
    })
  }

  setCancelJobSetsDialogState(dialogState: CancelJobSetsDialogState) {
    this.setState({
      ...this.state,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        dialogState: dialogState,
      },
    })
  }

  setReprioritizeJobSetsDialogState(dialogState: ReprioritizeJobSetsDialogState) {
    this.setState({
      ...this.state,
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        dialogState: dialogState,
      },
    })
  }

  async cancelJobs() {
    if (this.state.cancelJobSetsDialogContext.cancelJobSetsRequestStatus === "Loading") {
      return
    }

    this.setState({
      ...this.state,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        cancelJobSetsRequestStatus: "Loading",
      },
    })

    const cancelJobSetsResult = await this.props.jobService.cancelJobSets(
      this.state.queue,
      this.state.cancelJobSetsDialogContext.jobSetsToCancel,
    )

    this.setState({
      ...this.state,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: cancelJobSetsResult.failedJobSetCancellations.map((failed) => failed.jobSet),
        cancelJobSetsResult: cancelJobSetsResult,
        dialogState: "CancelJobSetsResult",
        cancelJobSetsRequestStatus: "Idle",
      },
    })

    if (cancelJobSetsResult.failedJobSetCancellations.length === 0) {
      // All succeeded
      await this.loadJobSets()
    }
  }

  async reprioritizeJobSets() {
    if (this.state.reprioritizeJobSetsDialogContext.reproiritizeJobSetsRequestStatus === "Loading") {
      return
    }

    this.setState({
      ...this.state,
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        reproiritizeJobSetsRequestStatus: "Loading",
      },
    })

    const reprioritizeJobSetsResult = await this.props.jobService.reprioritizeJobSets(
      this.state.queue,
      this.state.reprioritizeJobSetsDialogContext.jobSetsToReprioritize,
      this.state.reprioritizeJobSetsDialogContext.newPriority,
    )

    this.setState({
      ...this.state,
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        jobSetsToReprioritize: reprioritizeJobSetsResult.failedJobSetReprioritizations.map((failed) => failed.jobSet),
        reprioritizeJobSetsResult: reprioritizeJobSetsResult,
        dialogState: "ReprioritizeJobSetsResult",
        reproiritizeJobSetsRequestStatus: "Idle",
      },
    })

    if (reprioritizeJobSetsResult.failedJobSetReprioritizations.length === 0) {
      // All succeeded
      await this.loadJobSets()
    }
  }

  handlePriorityChange(newValue: string) {
    const valid = newPriorityRegex.test(newValue) && newValue.length > 0
    this.setState({
      ...this.state,
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        isValid: valid,
        newPriority: Number(newValue),
      },
    })
  }

  setView(view: JobSetsView) {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(this.state.queue, view),
    })

    this.setState({
      ...this.state,
      currentView: view,
      selectedJobSets: new Map<string, JobSet>(),
    })
  }

  navigateToJobSetForState(jobSet: string, jobState: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/jobs",
      search: `queue=${this.state.queue}&job_set=${jobSet}&job_states=${jobState}`,
    })
  }

  toggleAutoRefresh(autoRefresh: boolean) {
    this.setState({
      ...this.state,
      autoRefresh: autoRefresh,
    })

    if (autoRefresh) {
      this.autoRefreshService.start()
    } else {
      this.autoRefreshService.stop()
    }
  }

  private async loadJobSets() {
    await setStateAsync(this, {
      ...this.state,
      getJobSetsRequestStatus: "Loading",
    })
    const jobSets = await this.fetchJobSets({
      queue: this.state.queue,
      newestFirst: this.state.newestFirst,
      activeOnly: this.state.activeOnly,
    })
    this.setState({
      ...this.state,
      jobSets: jobSets,
      getJobSetsRequestStatus: "Idle",
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: [],
      },
      reprioritizeJobSetsDialogContext: {
        ...this.state.reprioritizeJobSetsDialogContext,
        jobSetsToReprioritize: [],
      },
    })
  }

  private fetchJobSets(getJobSetsRequest: GetJobSetsRequest): Promise<JobSet[]> {
    console.log(`actually called with ${getJobSetsRequest.queue}`)
    return this.props.jobService.getJobSets(getJobSetsRequest)
  }

  private setStateAsync(state: JobSetsContainerState): Promise<void> {
    return new Promise((resolve) => this.setState(state, resolve))
  }

  private static getCancellableSelectedJobSets(selectedJobSets: Map<string, JobSet>): JobSet[] {
    return Array.from(selectedJobSets.values()).filter(
      (jobSet) => jobSet.jobsQueued > 0 || jobSet.jobsPending > 0 || jobSet.jobsRunning > 0,
    )
  }

  private static getReprioritizeableJobSets(selectedJobSets: Map<string, JobSet>): JobSet[] {
    return Array.from(selectedJobSets.values()).filter((jobSet) => jobSet.jobsQueued > 0)
  }

  render() {
    return (
      <Fragment>
        <CancelJobSetsDialog
          dialogState={this.state.cancelJobSetsDialogContext.dialogState}
          queue={this.state.queue}
          jobSetsToCancel={this.state.cancelJobSetsDialogContext.jobSetsToCancel}
          cancelJobSetsResult={this.state.cancelJobSetsDialogContext.cancelJobSetsResult}
          cancelJobSetsRequestStatus={this.state.cancelJobSetsDialogContext.cancelJobSetsRequestStatus}
          onCancelJobSets={this.cancelJobs}
          onClose={() => this.setCancelJobSetsDialogState("None")}
        />
        <ReprioritizeJobSetsDialog
          dialogState={this.state.reprioritizeJobSetsDialogContext.dialogState}
          queue={this.state.queue}
          isValid={this.state.reprioritizeJobSetsDialogContext.isValid}
          newPriority={this.state.reprioritizeJobSetsDialogContext.newPriority}
          jobSetsToReprioritize={this.state.reprioritizeJobSetsDialogContext.jobSetsToReprioritize}
          reprioritizeJobSetsResult={this.state.reprioritizeJobSetsDialogContext.reprioritizeJobSetsResult}
          reproiritizeJobSetsRequestStatus={
            this.state.reprioritizeJobSetsDialogContext.reproiritizeJobSetsRequestStatus
          }
          onReprioritizeJobSets={this.reprioritizeJobSets}
          onPriorityChange={this.handlePriorityChange}
          onClose={() => this.setReprioritizeJobSetsDialogState("None")}
        />
        <JobSets
          canCancel={
            this.state.currentView === "job-counts" && this.state.cancelJobSetsDialogContext.jobSetsToCancel.length > 0
          }
          canReprioritize={
            this.state.currentView === "job-counts" &&
            this.state.reprioritizeJobSetsDialogContext.jobSetsToReprioritize.length > 0
          }
          queue={this.state.queue}
          view={this.state.currentView}
          jobSets={this.state.jobSets}
          selectedJobSets={this.state.selectedJobSets}
          getJobSetsRequestStatus={this.state.getJobSetsRequestStatus}
          autoRefresh={this.state.autoRefresh}
          newestFirst={this.state.newestFirst}
          activeOnly={this.state.activeOnly}
          onQueueChange={this.setQueue}
          onViewChange={this.setView}
          onOrderChange={this.orderChange}
          onActiveOnlyChange={this.activeOnlyChange}
          onRefresh={this.loadJobSets}
          onJobSetClick={this.navigateToJobSetForState}
          onSelectJobSet={this.selectJobSet}
          onShiftSelectJobSet={this.shiftSelectJobSet}
          onDeselectAllClick={this.deselectAll}
          onCancelJobSetsClick={() => this.setCancelJobSetsDialogState("CancelJobSets")}
          onToggleAutoRefresh={this.toggleAutoRefresh}
          onReprioritizeJobSetsClick={() => this.setReprioritizeJobSetsDialogState("ReprioritizeJobSets")}
        />
      </Fragment>
    )
  }
}

export default withRouter(JobSetsContainer)
