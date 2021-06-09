import * as queryString from "querystring"

import React, { Fragment } from "react"

import { RouteComponentProps, withRouter } from "react-router-dom"

import CancelJobSetsDialog, {
  CancelJobSetsDialogContext,
  CancelJobSetsDialogState,
} from "../components/job-sets/CancelJobSetsDialog"
import JobSets from "../components/job-sets/JobSets"
import JobService, { JobSet } from "../services/JobService"
import { debounced } from "../utils"

type JobSetsContainerProps = {
  jobService: JobService
} & RouteComponentProps

type JobSetsContainerParams = {
  queue: string
  currentView: JobSetsView
}

export type CancelJobSetsRequestStatus = "Loading" | "Idle"

type JobSetsContainerState = {
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  cancelJobSetsDialogContext: CancelJobSetsDialogContext
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
  constructor(props: JobSetsContainerProps) {
    super(props)

    this.state = {
      queue: "",
      jobSets: [],
      currentView: "job-counts",
      selectedJobSets: new Map<string, JobSet>(),
      cancelJobSetsDialogContext: {
        dialogState: "None",
        queue: "",
        jobSetsToCancel: [],
        cancelJobSetsResult: { cancelledJobSets: [], failedJobSetCancellations: [] },
        cancelJobSetsRequestStatus: "Idle",
      },
    }

    this.setQueue = this.setQueue.bind(this)
    this.setView = this.setView.bind(this)
    this.refresh = this.refresh.bind(this)
    this.navigateToJobSetForState = this.navigateToJobSetForState.bind(this)
    this.selectJobSet = this.selectJobSet.bind(this)
    this.shiftSelectJobSet = this.shiftSelectJobSet.bind(this)
    this.deselectAll = this.deselectAll.bind(this)
    this.setCancelJobSetsDialogState = this.setCancelJobSetsDialogState.bind(this)
    this.cancelJobs = this.cancelJobs.bind(this)

    this.fetchJobSets = debounced(this.fetchJobSets.bind(this), 100)
  }

  async componentDidMount() {
    const params = getParamsFromQueryString(this.props.location.search)
    await this.setStateAsync({
      ...this.state,
      ...params,
    })

    await this.loadJobSets()
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

  async refresh() {
    await this.loadJobSets()
  }

  selectJobSet(jobSet: JobSet, selected: boolean) {
    const selectedJobSets = new Map<string, JobSet>(this.state.selectedJobSets)
    if (selected) {
      selectedJobSets.set(jobSet.jobSetId, jobSet)
    } else {
      if (selectedJobSets.has(jobSet.jobSetId)) {
        selectedJobSets.delete(jobSet.jobSetId)
      }
    }

    const cancellableJobSets = JobSetsContainer.getCancellableSelectedJobSets(selectedJobSets)
    this.setState({
      ...this.state,
      selectedJobSets: selectedJobSets,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: cancellableJobSets,
      },
    })
  }

  shiftSelectJobSet(index: number, selected: boolean) {
    if (index >= this.state.jobSets.length || index < 0) {
      return
    }
    const job = this.state.jobSets[index]

    if (!selected || this.state.selectedJobSets.size === 0) {
      return this.selectJobSet(job, selected)
    }

    let firstSelectedIndex = 0
    for (let i = 0; i < this.state.jobSets.length; i++) {
      if (this.state.selectedJobSets.has(this.state.jobSets[i].jobSetId)) {
        firstSelectedIndex = i
        break
      }
    }

    const selectedJobSets = new Map<string, JobSet>(this.state.selectedJobSets)
    let start = firstSelectedIndex
    let end = index
    if (index < firstSelectedIndex) {
      start = index
      end = firstSelectedIndex
    }

    for (let i = start; i <= end; i++) {
      selectedJobSets.set(this.state.jobSets[i].jobSetId, this.state.jobSets[i])
    }

    const cancellableJobSets = JobSetsContainer.getCancellableSelectedJobSets(selectedJobSets)
    this.setState({
      ...this.state,
      selectedJobSets: selectedJobSets,
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: cancellableJobSets,
      },
    })
  }

  deselectAll() {
    this.setState({
      ...this.state,
      selectedJobSets: new Map<string, JobSet>(),
      cancelJobSetsDialogContext: {
        ...this.state.cancelJobSetsDialogContext,
        jobSetsToCancel: [],
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

    if (cancelJobSetsResult.cancelledJobSets.length > 0) {
      // Some succeed
      await this.loadJobSets()
    }
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

  private async loadJobSets() {
    const jobSets = await this.props.jobService.getJobSets(this.state.queue)
    this.setState({
      ...this.state,
      selectedJobSets: new Map<string, JobSet>(),
      jobSets: jobSets,
    })
  }

  private fetchJobSets(queue: string): Promise<JobSet[]> {
    return this.props.jobService.getJobSets(queue)
  }

  private setStateAsync(state: JobSetsContainerState): Promise<void> {
    return new Promise((resolve) => this.setState(state, resolve))
  }

  private static getCancellableSelectedJobSets(selectedJobSets: Map<string, JobSet>): JobSet[] {
    return Array.from(selectedJobSets.values()).filter(
      (jobSet) => jobSet.jobsQueued > 0 || jobSet.jobsPending > 0 || jobSet.jobsRunning > 0,
    )
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
        <JobSets
          canCancel={
            this.state.currentView === "job-counts" && this.state.cancelJobSetsDialogContext.jobSetsToCancel.length > 0
          }
          queue={this.state.queue}
          view={this.state.currentView}
          jobSets={this.state.jobSets}
          selectedJobSets={this.state.selectedJobSets}
          onQueueChange={this.setQueue}
          onViewChange={this.setView}
          onRefresh={this.refresh}
          onJobSetClick={this.navigateToJobSetForState}
          onSelectJobSet={this.selectJobSet}
          onShiftSelectJobSet={this.shiftSelectJobSet}
          onDeselectAllClick={this.deselectAll}
          onCancelJobSetsClick={() => this.setCancelJobSetsDialogState("CancelJobSets")}
        />
      </Fragment>
    )
  }
}

export default withRouter(JobSetsContainer)
