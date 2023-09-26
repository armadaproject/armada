import React from "react"

import JobDialog from "./JobDialog"
import Overview from "../components/Overview"
import IntervalService from "../services/IntervalService"
import { JobService, Job, QueueInfo } from "../services/JobService"
import LogService from "../services/LogService"
import OverviewLocalStorageService from "../services/OverviewLocalStorageService"
import { PropsWithRouter, RequestStatus, setStateAsync, withRouter } from "../utils"

interface OverviewContainerProps extends PropsWithRouter {
  jobService: JobService
  logService: LogService
  overviewAutoRefreshMs: number
}

export type OverviewContainerState = {
  queueInfos: QueueInfo[]
  openQueueMenu: string
  queueMenuAnchor: HTMLElement | null
  overviewRequestStatus: RequestStatus
  autoRefresh: boolean
  jobDetailsIsOpen: boolean
  clickedJob?: Job
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
      jobDetailsIsOpen: false,
    }

    this.fetchOverview = this.fetchOverview.bind(this)
    this.setOpenQueueMenu = this.setOpenQueueMenu.bind(this)
    this.navigateToJobSets = this.navigateToJobSets.bind(this)
    this.navigateToJobs = this.navigateToJobs.bind(this)
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this)

    this.openModalForJob = this.openModalForJob.bind(this)
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
    this.props.router.navigate({
      pathname: "/job-sets",
      search: `queue=${queue}`,
    })
  }

  navigateToJobs(queue: string) {
    this.props.router.navigate({
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
        jobDetailsIsOpen: true,
        clickedJob: job,
      })
    }
  }

  closeModal() {
    this.setState({
      ...this.state,
      jobDetailsIsOpen: false,
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
      <>
        <JobDialog
          isOpen={this.state.jobDetailsIsOpen}
          job={this.state.clickedJob}
          logService={this.props.logService}
          onClose={this.closeModal}
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
      </>
    )
  }
}

export default withRouter((props: OverviewContainerProps) => {
  return <OverviewContainer {...props} />
})
