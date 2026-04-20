import { Component, FC } from "react"

import { ErrorBoundary } from "react-error-boundary"

import { StandardColumnId } from "../../../common/jobsTableColumns"
import {
  ApiResult,
  debounced,
  PropsWithRouter,
  RequestStatus,
  selectItem,
  setStateAsync,
  withRouter,
} from "../../../common/utils"
import { AlertErrorFallback } from "../../../components/AlertErrorFallback"
import { GetJobSetsRequest, JobSet, JobSetsOrderByColumn, JobState, Match } from "../../../models/lookoutModels"
import { JOBS } from "../../../pathnames"
import JobSetsLocalStorageService from "../../../services/JobSetsLocalStorageService"
import JobSetsQueryParamsService from "../../../services/JobSetsQueryParamsService"
import {
  DEFAULT_PREFERENCES,
  JobsTablePreferences,
  stringifyQueryParams,
  toQueryStringSafe,
} from "../../../services/lookout/JobsTablePreferencesService"
import { useGroupJobs } from "../../../services/lookout/useGroupJobs"

import CancelJobSetsDialog, { getCancellableJobSets } from "./CancelJobSetsDialog"
import JobSets from "./JobSets"
import ReprioritizeJobSetsDialog, { getReprioritizableJobSets } from "./ReprioritizeJobSetsDialog"

interface JobSetsContainerProps extends PropsWithRouter {
  groupJobs: ReturnType<typeof useGroupJobs>
  jobSetsAutoRefreshMs: number | undefined
}

type JobSetsContainerParams = {
  queue: string
}

export type JobSetsContainerState = {
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  getJobSetsRequestStatus: RequestStatus
  autoRefresh: boolean
  lastSelectedIndex: number
  orderByColumn: JobSetsOrderByColumn
  orderByDesc: boolean
  activeOnly: boolean
  cancelJobSetsIsOpen: boolean
  reprioritizeJobSetsIsOpen: boolean
} & JobSetsContainerParams

class JobSetsContainer extends Component<JobSetsContainerProps, JobSetsContainerState> {
  autoRefreshInterval: NodeJS.Timeout | undefined
  autoRefreshMs: number | undefined
  localStorageService: JobSetsLocalStorageService
  queryParamsService: JobSetsQueryParamsService

  constructor(props: JobSetsContainerProps) {
    super(props)

    this.autoRefreshMs = props.jobSetsAutoRefreshMs
    this.localStorageService = new JobSetsLocalStorageService()
    this.queryParamsService = new JobSetsQueryParamsService(this.props.router)

    this.state = {
      queue: "",
      jobSets: [],
      selectedJobSets: new Map<string, JobSet>(),
      getJobSetsRequestStatus: "Idle",
      autoRefresh: true,
      lastSelectedIndex: 0,
      cancelJobSetsIsOpen: false,
      reprioritizeJobSetsIsOpen: false,
      orderByColumn: "submitted",
      orderByDesc: true,
      activeOnly: false,
    }

    this.setQueue = this.setQueue.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.activeOnlyChange = this.activeOnlyChange.bind(this)
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
    this.onJobSetStateClick = this.onJobSetStateClick.bind(this)
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

    this.tryStartAutoRefresh()
  }

  componentWillUnmount() {
    this.stopAutoRefresh()
  }

  async setQueue(queue: string) {
    await this.updateState({
      ...this.state,
      queue: queue,
    })

    // Performed separately because debounced
    await this.loadJobSets()
  }

  async orderChange(orderByColumn: JobSetsOrderByColumn, orderByDesc: boolean) {
    await this.updateState({
      ...this.state,
      orderByColumn,
      orderByDesc,
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

  async toggleAutoRefresh(autoRefresh: boolean) {
    await this.updateState({
      ...this.state,
      autoRefresh: autoRefresh,
    })
    this.tryStartAutoRefresh()
  }

  tryStartAutoRefresh() {
    this.stopAutoRefresh()
    if (this.state.autoRefresh && this.autoRefreshMs !== undefined) {
      this.autoRefreshInterval = setInterval(this.loadJobSets, this.autoRefreshMs)
    }
  }

  stopAutoRefresh() {
    if (this.autoRefreshInterval) {
      clearInterval(this.autoRefreshInterval)
      this.autoRefreshInterval = undefined
    }
  }

  private async onJobSetStateClick(rowIndex: number, state: string) {
    const jobSet = this.state.jobSets[rowIndex]

    const prefs: JobsTablePreferences = {
      ...DEFAULT_PREFERENCES,
      filters: [
        {
          id: StandardColumnId.Queue,
          value: jobSet.queue,
        },
        {
          id: StandardColumnId.State,
          value: [state],
        },
        {
          id: StandardColumnId.JobSet,
          value: jobSet.jobSetId,
        },
      ],
    }

    this.props.router.navigate({
      pathname: JOBS,
      search: stringifyQueryParams(toQueryStringSafe(prefs)),
    })
  }

  private async updateState(updatedState: JobSetsContainerState) {
    this.localStorageService.saveState(updatedState)
    this.queryParamsService.saveState(updatedState)
    await setStateAsync(this, updatedState)
  }

  private async loadJobSets() {
    if (this.state.queue === "") {
      return
    }
    await setStateAsync(this, {
      ...this.state,
      getJobSetsRequestStatus: "Loading",
    })
    const jobSets = await this.fetchJobSets({
      queue: this.state.queue,
      orderByColumn: this.state.orderByColumn,
      orderByDesc: this.state.orderByDesc,
      activeOnly: this.state.activeOnly,
    })
    this.setState({
      ...this.state,
      jobSets: jobSets,
      getJobSetsRequestStatus: "Idle",
    })
  }

  private async fetchJobSets(getJobSetsRequest: GetJobSetsRequest): Promise<JobSet[]> {
    const response = await this.props.groupJobs(
      [
        {
          isAnnotation: false,
          field: "queue",
          value: getJobSetsRequest.queue,
          match: Match.Exact,
        },
      ],
      getJobSetsRequest.activeOnly,
      {
        field: getJobSetsRequest.orderByColumn,
        direction: getJobSetsRequest.orderByDesc ? "DESC" : "ASC",
      },
      {
        field: "jobSet",
        isAnnotation: false,
      },
      ["state", "submitted"],
      0,
      0,
    )

    return response.groups.map((group) => {
      const state = group.aggregates.state as Record<string, number>
      return {
        jobSetId: group.name,
        queue: getJobSetsRequest.queue,
        jobsQueued: state[JobState.Queued] || 0,
        jobsPending: state[JobState.Pending] || 0,
        jobsRunning: state[JobState.Running] || 0,
        jobsSucceeded: state[JobState.Succeeded] || 0,
        jobsFailed: state[JobState.Failed] || 0,
        jobsCancelled: state[JobState.Cancelled] || 0,
        latestSubmissionTime: group.aggregates.submitted as string,
      }
    })
  }

  render() {
    const selectedJobSets = Array.from(this.state.selectedJobSets.values())
    return (
      <>
        <CancelJobSetsDialog
          isOpen={this.state.cancelJobSetsIsOpen}
          queue={this.state.queue}
          selectedJobSets={selectedJobSets}
          onResult={this.handleApiResult}
          onClose={() => this.openCancelJobSets(false)}
        />
        <ReprioritizeJobSetsDialog
          isOpen={this.state.reprioritizeJobSetsIsOpen}
          queue={this.state.queue}
          selectedJobSets={selectedJobSets}
          onResult={this.handleApiResult}
          onClose={() => this.openReprioritizeJobSets(false)}
        />
        <ErrorBoundary FallbackComponent={AlertErrorFallback}>
          <JobSets
            canCancel={getCancellableJobSets(selectedJobSets).length > 0}
            canReprioritize={getReprioritizableJobSets(selectedJobSets).length > 0}
            queue={this.state.queue}
            jobSets={this.state.jobSets}
            selectedJobSets={this.state.selectedJobSets}
            getJobSetsRequestStatus={this.state.getJobSetsRequestStatus}
            autoRefresh={this.state.autoRefresh}
            orderByColumn={this.state.orderByColumn}
            orderByDesc={this.state.orderByDesc}
            activeOnly={this.state.activeOnly}
            onQueueChange={this.setQueue}
            onOrderChange={this.orderChange}
            onActiveOnlyChange={this.activeOnlyChange}
            onRefresh={this.loadJobSets}
            onSelectJobSet={this.selectJobSet}
            onShiftSelectJobSet={this.shiftSelectJobSet}
            onDeselectAllClick={this.deselectAll}
            onSelectAllClick={this.selectAll}
            onCancelJobSetsClick={() => this.openCancelJobSets(true)}
            onToggleAutoRefresh={this.autoRefreshMs !== undefined ? this.toggleAutoRefresh : undefined}
            onReprioritizeJobSetsClick={() => this.openReprioritizeJobSets(true)}
            onJobSetStateClick={this.onJobSetStateClick}
          />
        </ErrorBoundary>
      </>
    )
  }
}

const withGroupJobs = <T extends { groupJobs: ReturnType<typeof useGroupJobs> }>(
  Component: FC<T>,
): FC<Omit<T, "groupJobs">> => {
  function ComponentWithGroupJobs(props: T) {
    const groupJobs = useGroupJobs()
    return <Component {...props} groupJobs={groupJobs} />
  }
  return ComponentWithGroupJobs as FC<Omit<T, "groupJobs">>
}

export default withGroupJobs(withRouter((props: JobSetsContainerProps) => <JobSetsContainer {...props} />))
