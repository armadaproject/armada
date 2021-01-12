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
  modalState: ModalState
  modalQueue: string
}

export type ModalState = "OldestQueued" | "LongestRunning" | "None"

class OverviewContainer extends React.Component<OverviewContainerProps, OverviewContainerState> {
  constructor(props: OverviewContainerProps) {
    super(props)
    this.state = {
      queueInfos: [],
      modalState: "None",
      modalQueue: "",
    }

    this.navigateToQueueInJobs = this.navigateToQueueInJobs.bind(this)
    this.fetchQueueInfos = this.fetchQueueInfos.bind(this)
    this.setModalState = this.setModalState.bind(this)
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

  setModalState(modalState: ModalState, modalQueue: string) {
    this.setState({
      ...this.state,
      modalState: modalState,
      modalQueue: modalQueue,
    })
  }

  render() {
    return (
      <Overview
        queueInfos={this.state.queueInfos}
        modalState={this.state.modalState}
        modalQueue={this.state.modalQueue}
        onQueueInfoClick={this.navigateToQueueInJobs}
        onSetModalState={this.setModalState}
        onRefresh={this.fetchQueueInfos}/>
    )
  }
}

export default withRouter(OverviewContainer)
