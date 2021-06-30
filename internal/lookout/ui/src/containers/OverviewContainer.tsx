import React, { Fragment } from "react"

import { RouteComponentProps, withRouter } from "react-router-dom"

import Overview from "../components/Overview"
import JobDetailsModal, { JobDetailsModalContext, toggleExpanded } from "../components/job-details/JobDetailsModal"
import JobService, { Job, QueueInfo } from "../services/JobService"
import LogService from "../services/LogService"

type OverviewContainerProps = {
  jobService: JobService
  logService: LogService
} & RouteComponentProps

interface OverviewContainerState {
  queueInfos: QueueInfo[]
  openQueueMenu: string
  queueMenuAnchor: HTMLElement | null
  modalContext: JobDetailsModalContext
}

class OverviewContainer extends React.Component<OverviewContainerProps, OverviewContainerState> {
  constructor(props: OverviewContainerProps) {
    super(props)
    this.state = {
      queueInfos: [],
      openQueueMenu: "",
      queueMenuAnchor: null,
      modalContext: {
        open: false,
        selectTab: "detail",
        expandedItems: new Set(),
      },
    }

    this.fetchQueueInfos = this.fetchQueueInfos.bind(this)
    this.setOpenQueueMenu = this.setOpenQueueMenu.bind(this)
    this.navigateToJobSets = this.navigateToJobSets.bind(this)
    this.navigateToJobs = this.navigateToJobs.bind(this)

    this.openModalForJob = this.openModalForJob.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)
    this.closeModal = this.closeModal.bind(this)
  }

  async componentDidMount() {
    await this.fetchQueueInfos()
  }

  async fetchQueueInfos() {
    const queueInfos = await this.props.jobService.getOverview()
    this.setState({
      queueInfos: queueInfos,
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
          selectTab: "detail",
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
          onRefresh={this.fetchQueueInfos}
          onJobClick={this.openModalForJob}
          onSetQueueMenu={this.setOpenQueueMenu}
          onQueueMenuJobSetsClick={this.navigateToJobSets}
          onQueueMenuJobsClick={this.navigateToJobs}
        />
      </Fragment>
    )
  }
}

export default withRouter(OverviewContainer)
