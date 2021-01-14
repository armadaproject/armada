import React from 'react'
import * as H from "history";
import { match, withRouter } from 'react-router-dom';

import Overview from "../components/Overview"
import JobService, { QueueInfo } from "../services/JobService";

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
  openQueueMenu: string
  queueMenuAnchor: HTMLElement | null
}

export type ModalState = "OldestQueued" | "LongestRunning" | "None"

class OverviewContainer extends React.Component<OverviewContainerProps, OverviewContainerState> {
  constructor(props: OverviewContainerProps) {
    super(props)
    this.state = {
      queueInfos: [],
      modalState: "None",
      modalQueue: "",
      openQueueMenu: "",
      queueMenuAnchor: null,
    }

    this.fetchQueueInfos = this.fetchQueueInfos.bind(this)
    this.setModalState = this.setModalState.bind(this)
    this.setOpenQueueMenu = this.setOpenQueueMenu.bind(this)
    this.navigateToJobSets = this.navigateToJobSets.bind(this)
    this.navigateToJobs = this.navigateToJobs.bind(this)
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

  setModalState(modalState: ModalState, modalQueue: string) {
    this.setState({
      ...this.state,
      modalState: modalState,
      modalQueue: modalQueue,
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

  render() {
    return (
      <Overview
        queueInfos={this.state.queueInfos}
        modalState={this.state.modalState}
        modalQueue={this.state.modalQueue}
        openQueueMenu={this.state.openQueueMenu}
        queueMenuAnchor={this.state.queueMenuAnchor}
        onRefresh={this.fetchQueueInfos}
        onSetModalState={this.setModalState}
        onSetQueueMenu={this.setOpenQueueMenu}
        onQueueMenuJobSetsClick={this.navigateToJobSets}
        onQueueMenuJobsClick={this.navigateToJobs} />
    )
  }
}

export default withRouter(OverviewContainer)
