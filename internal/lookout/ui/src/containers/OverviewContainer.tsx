import React, { Fragment } from "react"

import { RouteComponentProps, withRouter } from "react-router-dom"

import Overview from "../components/Overview"
import JobDetailsModal, { JobDetailsModalContext, toggleExpanded } from "../components/job-details/JobDetailsModal"
import IntervalService from "../services/IntervalService"
import JobService, { Job, QueueInfo } from "../services/JobService"
import LogService from "../services/LogService"
import OverviewLocalStorageService from "../services/OverviewLocalStorageService"
import { RequestStatus, setStateAsync } from "../utils"

type OverviewContainerProps = {
  jobService: JobService
  logService: LogService
  overviewAutoRefreshMs: number
} & RouteComponentProps

export type OverviewContainerState = {
  queueInfos: QueueInfo[]
  openQueueMenu: string
  queueMenuAnchor: HTMLElement | null
  overviewRequestStatus: RequestStatus
  autoRefresh: boolean
  modalContext: JobDetailsModalContext
}

class OverviewContainer extends React.Component<OverviewContainerProps, OverviewContainerState> {
  autoRefreshService: IntervalService
  localStorageService: OverviewLocalStorageService

  constructor(props: OverviewContainerProps) {
    super(props)

    this.autoRefreshService = new IntervalService(props.overviewAutoRefreshMs)
    this.localStorageService = new OverviewLocalStorageService()

    this.state = {
      queueInfos: [],
      openQueueMenu: "",
      queueMenuAnchor: null,
      overviewRequestStatus: "Idle",
      autoRefresh: true,
      modalContext: {
        open: false,
        expandedItems: new Set(),
      },
    }

    this.fetchOverview = this.fetchOverview.bind(this)
    this.setOpenQueueMenu = this.setOpenQueueMenu.bind(this)
    this.navigateToJobSets = this.navigateToJobSets.bind(this)
    this.navigateToJobs = this.navigateToJobs.bind(this)
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this)

    this.openModalForJob = this.openModalForJob.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)
    this.closeModal = this.closeModal.bind(this)
  }

  async componentDidMount() {
    const newState = { ...this.state }
    this.localStorageService.updateState(newState)
    this.localStorageService.saveState(newState)
    await setStateAsync(this, newState)

    await this.fetchOverview()

    this.autoRefreshService.registerCallback(this.fetchOverview)
    this.tryStartAutoRefreshService()
  }

  componentWillUnmount() {
    this.autoRefreshService.stop()
  }

  async fetchOverview() {
    await setStateAsync(this, {
      ...this.state,
      overviewRequestStatus: "Loading",
    })
    const queueInfos = await this.props.jobService.getOverview()
    this.setState({
      queueInfos: queueInfos,
      overviewRequestStatus: "Idle",
    })
  }

  setOpenQueueMenu(queue: string, anchor: HTMLElement | null) {
    this.setState({
      ...this.state,
      openQueueMenu: queue,
      queueMenuAnchor: anchor,
    })
  }

  navigateToJobSets(queue: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/job-sets",
      search: `queue=${queue}`,
    })
  }

  navigateToJobs(queue: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/jobs",
      search: `queue=${queue}&job_states=Queued,Pending,Running`,
    })
  }

  openModalForJob(jobId: string, queue: string) {
    const queueInfo = this.state.queueInfos.find((queueInfo) => queueInfo.queue === queue)
    if (!queueInfo) {
      return
    }

    let job: Job | undefined
    if (queueInfo.oldestQueuedJob && jobId === queueInfo.oldestQueuedJob.jobId) {
      job = queueInfo.oldestQueuedJob
    } else if (queueInfo.longestRunningJob && jobId === queueInfo.longestRunningJob.jobId) {
      job = queueInfo.longestRunningJob
    }

    if (job) {
      this.setState({
        ...this.state,
        modalContext: {
          open: true,
          job: job,
          expandedItems: new Set(),
        },
      })
    }
  }

  // Toggle expanded items in scheduling history in Job detail modal
  toggleExpanded(item: string, isExpanded: boolean) {
    const newExpanded = toggleExpanded(item, isExpanded, this.state.modalContext.expandedItems)
    this.setState({
      ...this.state,
      modalContext: {
        ...this.state.modalContext,
        expandedItems: newExpanded,
      },
    })
  }

  closeModal() {
    this.setState({
      ...this.state,
      modalContext: {
        ...this.state.modalContext,
        open: false,
      },
    })
  }

  async toggleAutoRefresh(autoRefresh: boolean) {
    const newState = {
      ...this.state,
      autoRefresh: autoRefresh,
    }
    this.localStorageService.saveState(newState)
    await setStateAsync(this, newState)
    this.tryStartAutoRefreshService()
  }

  tryStartAutoRefreshService() {
    if (this.state.autoRefresh) {
      this.autoRefreshService.start()
    } else {
      this.autoRefreshService.stop()
    }
  }

  render() {
    return (
      <Fragment>
        <JobDetailsModal
          logService={this.props.logService}
          onToggleExpanded={this.toggleExpanded}
          onClose={this.closeModal}
          {...this.state.modalContext}
        />
        <Overview
          queueInfos={this.state.queueInfos}
          openQueueMenu={this.state.openQueueMenu}
          queueMenuAnchor={this.state.queueMenuAnchor}
          overviewRequestStatus={this.state.overviewRequestStatus}
          autoRefresh={this.state.autoRefresh}
          onRefresh={this.fetchOverview}
          onJobClick={this.openModalForJob}
          onSetQueueMenu={this.setOpenQueueMenu}
          onQueueMenuJobSetsClick={this.navigateToJobSets}
          onQueueMenuJobsClick={this.navigateToJobs}
          onToggleAutoRefresh={this.toggleAutoRefresh}
        />
      </Fragment>
    )
  }
}

export default withRouter(OverviewContainer)
