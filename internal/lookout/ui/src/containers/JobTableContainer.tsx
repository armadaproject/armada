import React from 'react'

import { JobInfoViewModel, JobService } from "../services/jobs"
import JobTable from "../components/JobTable"
import { updateArray } from "../utils";

type JobTableContainerProps = {
  jobService: JobService
}

type JobTableContainerState = {
  jobInfos: JobInfoViewModel[]
  canLoadMore: boolean
}

export class JobTableContainer extends React.Component<JobTableContainerProps, JobTableContainerState> {
  constructor(props: JobTableContainerProps) {
    super(props);
    this.state = { jobInfos: [], canLoadMore: true }

    this.loadJobInfos = this.loadJobInfos.bind(this)
    this.jobInfoIsLoaded = this.jobInfoIsLoaded.bind(this)
  }

  async loadJobInfos(start: number, stop: number): Promise<JobInfoViewModel[]> {
    const take = stop - start;
    const newJobInfos = await this.props.jobService.getJobsInQueue("test", take, start)

    let canLoadMore = true
    if (take > newJobInfos.length) {
      // No more to be fetched from API
      canLoadMore = false
    }

    updateArray(this.state.jobInfos, newJobInfos, start, stop)
    this.setState({
      jobInfos: this.state.jobInfos,
      canLoadMore: canLoadMore
    })
    return this.state.jobInfos
  }

  jobInfoIsLoaded(index: number) {
    return !!this.state.jobInfos[index]
  }

  render() {
    return <JobTable
      jobInfos={this.state.jobInfos}
      fetchJobs={this.loadJobInfos}
      isLoaded={this.jobInfoIsLoaded}
      canLoadMore={this.state.canLoadMore}/>
  }
}
