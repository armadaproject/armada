import React from 'react'
import * as H from "history";
import { match, withRouter } from 'react-router-dom';

import JobService, { QueueInfo } from "../services/JobService";
import Overview from "../components/Overview"

interface OverviewContainerProps {
  jobService: JobService
  history: H.History
  location: H.Location
  match: match
}

interface OverviewContainerState {
  queueInfos: QueueInfo[]
}

class OverviewContainer extends React.Component<OverviewContainerProps, OverviewContainerState> {
  constructor(props: OverviewContainerProps) {
    super(props)
    this.state = {
      queueInfos: []
    }

    this.navigateToQueueInJobs = this.navigateToQueueInJobs.bind(this)
    this.fetchQueueInfos = this.fetchQueueInfos.bind(this)
  }

  async componentDidMount() {
    await this.fetchQueueInfos()
  }

  navigateToQueueInJobs(queue: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/jobs",
      search: `queue=${queue}&job_states=Queued,Pending,Running`,
    })
  }

  async fetchQueueInfos() {
    const queueInfos = await this.props.jobService.getOverview()
    this.setState({
      queueInfos: queueInfos
    })
  }

  render() {
    return (
      <Overview
        queueInfos={this.state.queueInfos}
        onQueueInfoClick={this.navigateToQueueInJobs}
        onRefresh={this.fetchQueueInfos}/>
    )
  }
}

export default withRouter(OverviewContainer)
