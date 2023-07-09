import React from "react"

import CancelJobSetsDialog, { getCancellableJobSets } from "./CancelJobSetsDialog"
import ReprioritizeJobSetsDialog, { getReprioritizeableJobSets } from "./ReprioritizeJobSetsDialog"
import JobSets from "../components/job-sets/JobSets"
import IntervalService from "../services/IntervalService"
import { JobService, GetJobSetsRequest, JobSet } from "../services/JobService"
import JobSetsLocalStorageService from "../services/JobSetsLocalStorageService"
import JobSetsQueryParamsService from "../services/JobSetsQueryParamsService"
import { ApiResult, debounced, PropsWithRouter, RequestStatus, selectItem, setStateAsync, withRouter } from "../utils"

interface JobSetsContainerProps extends PropsWithRouter {
  jobService: JobService
  jobSetsAutoRefreshMs: number
}

type JobSetsContainerParams = {
  queue: string
  currentView: JobSetsView
}

export type JobSetsContainerState = {
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  getJobSetsRequestStatus: RequestStatus
  autoRefresh: boolean
  lastSelectedIndex: number
  newestFirst: boolean
  activeOnly: boolean
  cancelJobSetsIsOpen: boolean
  reprioritizeJobSetsIsOpen: boolean
} & JobSetsContainerParams

export type JobSetsView = "job-counts" | "runtime" | "queued-time"

export function isJobSetsView(val: string): val is JobSetsView {
  return ["job-counts", "runtime", "queued-time"].includes(val)
}

class JobSetsContainer extends React.Component<JobSetsContainerProps, JobSetsContainerState> {
  autoRefreshService: IntervalService
  localStorageService: JobSetsLocalStorageService
  queryParamsService: JobSetsQueryParamsService

  constructor(props: JobSetsContainerProps) {
    super(props)

    this.autoRefreshService = new IntervalService(props.jobSetsAutoRefreshMs)
    this.localStorageService = new JobSetsLocalStorageService()
    this.queryParamsService = new JobSetsQueryParamsService(this.props.router)

    this.state = {
      queue: "",
      jobSets: [],
      currentView: "job-counts",
      selectedJobSets: new Map<string, JobSet>(),
      getJobSetsRequestStatus: "Idle",
      autoRefresh: true,
      lastSelectedIndex: 0,
      cancelJobSetsIsOpen: false,
      reprioritizeJobSetsIsOpen: false,
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
    this.selectAll = this.selectAll.bind(this)

    this.openCancelJobSets = this.openCancelJobSets.bind(this)
    this.openReprioritizeJobSets = this.openReprioritizeJobSets.bind(this)
    this.handleApiResult = this.handleApiResult.bind(this)

    this.fetchJobSets = debounced(this.fetchJobSets.bind(this), 100)
    this.loadJobSets = this.loadJobSets.bind(this)
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this)
  }

  async componentDidMount() {
    const newState = { ...this.state }

    this.localStorageService.updateState(newState)
    this.queryParamsService.updateState(newState)

    this.localStorageService.saveState(newState)
    // queryParamsService.saveState calls navigate, which should only be called in useEffect
    // actual fix is migrating this component to a functional one with hooks
    setTimeout(() => this.queryParamsService.saveState(newState))

    await setStateAsync(this, {
      ...newState,
    })

    await this.loadJobSets()

    this.autoRefreshService.registerCallback(this.loadJobSets)
    this.tryStartAutoRefreshService()
  }

  componentWillUnmount() {
    this.autoRefreshService.stop()
  }

  async setQueue(queue: string) {
    await this.updateState({
      ...this.state,
      queue: queue,
    })

    // Performed separately because debounced
    await this.loadJobSets()
  }

  async orderChange(newestFirst: boolean) {
    await this.updateState({
      ...this.state,
      newestFirst: newestFirst,
    })
    await this.loadJobSets()
  }

  async activeOnlyChange(activeOnly: boolean) {
    await this.updateState({
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

    this.setState({
      ...this.state,
      selectedJobSets: selectedJobSets,
      lastSelectedIndex: index,
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

    this.setState({
      ...this.state,
      selectedJobSets: selectedJobSets,
      lastSelectedIndex: index,
    })
  }

  deselectAll() {
    this.setState({
      ...this.state,
      selectedJobSets: new Map<string, JobSet>(),
      lastSelectedIndex: 0,
    })
  }

  selectAll() {
    const selected = new Map<string, JobSet>()
    this.state.jobSets.forEach((jobSet) => selected.set(jobSet.jobSetId, jobSet))

    this.setState({
      ...this.state,
      selectedJobSets: selected,
      lastSelectedIndex: 0,
    })
  }

  openCancelJobSets(isOpen: boolean) {
    this.setState({
      ...this.state,
      cancelJobSetsIsOpen: isOpen,
    })
  }

  openReprioritizeJobSets(isOpen: boolean) {
    this.setState({
      ...this.state,
      reprioritizeJobSetsIsOpen: isOpen,
    })
  }

  handleApiResult(result: ApiResult) {
    if (result === "Success") {
      this.deselectAll()
      return this.loadJobSets()
    } else if (result === "Partial success") {
      return this.loadJobSets()
    }
  }

  setView(view: JobSetsView) {
    this.updateState({
      ...this.state,
      currentView: view,
      selectedJobSets: new Map<string, JobSet>(),
    })
  }

  navigateToJobSetForState(jobSet: string, jobState: string) {
    this.props.router.navigate({
      pathname: "/jobs",
      search: `queue=${this.state.queue}&job_set=${jobSet}&job_states=${jobState}`,
    })
  }

  async toggleAutoRefresh(autoRefresh: boolean) {
    await this.updateState({
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

  private async updateState(updatedState: JobSetsContainerState) {
    this.localStorageService.saveState(updatedState)
    this.queryParamsService.saveState(updatedState)
    await setStateAsync(this, updatedState)
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
    })
  }

  private fetchJobSets(getJobSetsRequest: GetJobSetsRequest): Promise<JobSet[]> {
    return this.props.jobService.getJobSets(getJobSetsRequest)
  }

  render() {
    const selectedJobSets = Array.from(this.state.selectedJobSets.values())
    return (
      <>
        <CancelJobSetsDialog
          isOpen={this.state.cancelJobSetsIsOpen}
          queue={this.state.queue}
          selectedJobSets={selectedJobSets}
          jobService={this.props.jobService}
          onResult={this.handleApiResult}
          onClose={() => this.openCancelJobSets(false)}
        />
        <ReprioritizeJobSetsDialog
          isOpen={this.state.reprioritizeJobSetsIsOpen}
          queue={this.state.queue}
          selectedJobSets={selectedJobSets}
          jobService={this.props.jobService}
          onResult={this.handleApiResult}
          onClose={() => this.openReprioritizeJobSets(false)}
        />
        <JobSets
          canCancel={this.state.currentView === "job-counts" && getCancellableJobSets(selectedJobSets).length > 0}
          canReprioritize={
            this.state.currentView === "job-counts" && getReprioritizeableJobSets(selectedJobSets).length > 0
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
          onSelectAllClick={this.selectAll}
          onCancelJobSetsClick={() => this.openCancelJobSets(true)}
          onToggleAutoRefresh={this.toggleAutoRefresh}
          onReprioritizeJobSetsClick={() => this.openReprioritizeJobSets(true)}
        />
      </>
    )
  }
}

export default withRouter((props: JobSetsContainerProps) => {
  return <JobSetsContainer {...props} />
})
