import React from 'react'

import JobService, { JobInfoViewModel } from "../services/JobService"
import Jobs from "../components/Jobs"
import { updateArray } from "../utils";

type JobsContainerProps = {
  jobService: JobService
}

type JobsContainerState = {
  jobInfos: JobInfoViewModel[]
  queue: string
  newestFirst: boolean
  canLoadMore: boolean
}

export class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
  constructor(props: JobsContainerProps) {
    super(props);
    this.state = { jobInfos: [], canLoadMore: true, queue: "", newestFirst: true }

    this.loadJobInfos = this.loadJobInfos.bind(this)
    this.jobInfoIsLoaded = this.jobInfoIsLoaded.bind(this)
    this.queueChange = this.queueChange.bind(this)
    this.orderChange = this.orderChange.bind(this)
    this.refresh = this.refresh.bind(this)
  }

  async loadJobInfos(start: number, stop: number): Promise<JobInfoViewModel[]> {
    const take = stop - start;
    const newJobInfos = await this.props.jobService.getJobsInQueue(
      this.state.queue,
      take,
      start,
      this.state.newestFirst,
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

  queueChange(queue: string) {
    console.log("queueChange", queue)
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      queue: queue,
    })
  }

  orderChange(newestFirst: boolean) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      newestFirst: newestFirst
    })
  }

  refresh() {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
    })
  }

  render() {
    return (
      <Jobs
        jobInfos={this.state.jobInfos}
        queue={this.state.queue}
        newestFirst={this.state.newestFirst}
        canLoadMore={this.state.canLoadMore}
        fetchJobs={this.loadJobInfos}
        isLoaded={this.jobInfoIsLoaded}
        onQueueChange={this.queueChange}
        onOrderChange={this.orderChange}
        onRefresh={this.refresh}/>
    )
  }
}
