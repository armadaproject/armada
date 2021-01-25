import React from 'react'
import { RouteComponentProps, withRouter } from 'react-router-dom';

import Overview from "../components/Overview"
import JobService, { QueueInfo } from "../services/JobService";

type OverviewContainerProps = {
  jobService: JobService
} & RouteComponentProps

interface OverviewContainerState {
  queueInfos: QueueInfo[]
  openQueueMenu: string
  queueMenuAnchor: HTMLElement | null
}

class OverviewContainer extends React.Component<OverviewContainerProps, OverviewContainerState> {
  constructor(props: OverviewContainerProps) {
    super(props)
    this.state = {
      queueInfos: [],
      openQueueMenu: "",
      queueMenuAnchor: null,
    }

    this.fetchQueueInfos = this.fetchQueueInfos.bind(this)
    this.setOpenQueueMenu = this.setOpenQueueMenu.bind(this)
    this.navigateToJobSets = this.navigateToJobSets.bind(this)
    this.navigateToJobs = this.navigateToJobs.bind(this)
    this.navigateToJobDetails = this.navigateToJobDetails.bind(this)
  }

  async componentDidMount() {
    await this.fetchQueueInfos()
  }

  async fetchQueueInfos() {
    const queueInfos = await this.props.jobService.getOverview()
    this.setState({
      queueInfos: queueInfos
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

  navigateToJobDetails(jobId: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/job-details",
      search: `id=${jobId}`
    })
  }

  render() {
    return (
      <Overview
        queueInfos={this.state.queueInfos}
        openQueueMenu={this.state.openQueueMenu}
        queueMenuAnchor={this.state.queueMenuAnchor}
        onRefresh={this.fetchQueueInfos}
        onJobClick={this.navigateToJobDetails}
        onSetQueueMenu={this.setOpenQueueMenu}
        onQueueMenuJobSetsClick={this.navigateToJobSets}
        onQueueMenuJobsClick={this.navigateToJobs} />
    )
  }
}

export default withRouter(OverviewContainer)
