import React from 'react'

import JobService, {
  JobInfoViewModel,
  JobStateViewModel,
  VALID_JOB_STATE_VIEW_MODELS
} from "../services/JobService"
import Jobs from "../components/Jobs"
import { updateArray } from "../utils";

type JobsContainerProps = {
  jobService: JobService
}

type JobsContainerState = {
  jobInfos: JobInfoViewModel[]
  queue: string
  jobSet: string
  jobStates: JobStateViewModel[]
  newestFirst: boolean
  canLoadMore: boolean
}

export class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
  constructor(props: JobsContainerProps) {
    super(props);
    this.state = {
      jobInfos: [],
      queue: "",
      jobSet: "",
      jobStates: VALID_JOB_STATE_VIEW_MODELS,
      newestFirst: true,
      canLoadMore: true,
    }

    this.loadJobInfos = this.loadJobInfos.bind(this)
    this.jobInfoIsLoaded = this.jobInfoIsLoaded.bind(this)
    this.queueChange = this.queueChange.bind(this)
    this.jobSetChange = this.jobSetChange.bind(this)
    this.jobStatesChange = this.jobStatesChange.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.refresh = this.refresh.bind(this)
  }

  async loadJobInfos(start: number, stop: number): Promise<JobInfoViewModel[]> {
    const take = stop - start;
    const newJobInfos = await this.props.jobService.getJobsInQueue(
      this.state.queue,
      take,
      start,
      [this.state.jobSet],
      this.state.newestFirst,
      this.state.jobStates,
    )

    let canLoadMore = true
    if (take > newJobInfos.length) {
      // No more to be fetched from API
      canLoadMore = false
    }

    updateArray(this.state.jobInfos, newJobInfos, start, stop)
    this.setState({
      ...this.state,
      jobInfos: this.state.jobInfos,
      canLoadMore: canLoadMore
    })
    return this.state.jobInfos
  }

  jobInfoIsLoaded(index: number) {
    return !!this.state.jobInfos[index]
  }

  queueChange(queue: string, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      queue: queue,
    }, callback)
  }

  jobSetChange(jobSet: string, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      jobSet: jobSet,
    }, callback)
  }

  jobStatesChange(jobStates: JobStateViewModel[], callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      jobStates: jobStates
    }, callback)
  }

  orderChange(newestFirst: boolean, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      newestFirst: newestFirst
    }, callback)
  }

  refresh(callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
    }, callback)
  }

  render() {
    return (
      <Jobs
        jobInfos={this.state.jobInfos}
        queue={this.state.queue}
        jobSet={this.state.jobSet}
        jobStates={this.state.jobStates}
        newestFirst={this.state.newestFirst}
        canLoadMore={this.state.canLoadMore}
        fetchJobs={this.loadJobInfos}
        isLoaded={this.jobInfoIsLoaded}
        onQueueChange={this.queueChange}
        onJobSetChange={this.jobSetChange}
        onJobStatesChange={this.jobStatesChange}
        onOrderChange={this.orderChange}
        onRefresh={this.refresh}/>
    )
  }
}
