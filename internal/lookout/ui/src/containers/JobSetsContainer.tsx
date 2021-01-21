import React from "react"
import { RouteComponentProps, withRouter } from "react-router-dom";
import * as queryString from "querystring";

import JobSets from "../components/JobSets";
import JobService, { JobSet } from "../services/JobService";
import { debounced } from "../utils";

type JobSetsContainerProps = {
  jobService: JobService
} & RouteComponentProps

interface JobSetsContainerState {
  queue: string
  jobSets: JobSet[]
}

function makeQueryString(queue: string): string {
  if (!queue) {
    return ""
  }

  const queryObject = {
    queue: queue
  }

  return queryString.stringify(queryObject)
}

function getQueueFromQueryString(query: string): string {
  if (query[0] === "?") {
    query = query.slice(1)
  }
  const params = queryString.parse(query) as { queue?: string }

  return params.queue ?? ""
}

class JobSetsContainer extends React.Component<JobSetsContainerProps, JobSetsContainerState> {
  constructor(props: JobSetsContainerProps) {
    super(props)

    this.state ={
      queue: "",
      jobSets: [],
    }

    this.setQueue = this.setQueue.bind(this)
    this.refresh = this.refresh.bind(this)
    this.navigateToJobSetForState = this.navigateToJobSetForState.bind(this)

    this.fetchJobSets = debounced(this.fetchJobSets.bind(this), 100)
  }

  async componentDidMount() {
    const queue = getQueueFromQueryString(this.props.location.search)
    const jobSets = await this.fetchJobSets(queue)

    this.setState({
      queue: queue,
      jobSets: jobSets,
    })
  }

  async setQueue(queue: string) {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(queue),
    })
    await this.setStateAsync({
      ...this.state,
      queue: queue,
    })

    // Performed separately because debounced
    const jobSets = await this.fetchJobSets(queue)
    this.setState({
      ...this.state,
      jobSets: jobSets,
    })
  }

  async refresh() {
    const jobSets = await this.fetchJobSets(this.state.queue)
    this.setState({
      ...this.state,
      jobSets: jobSets,
    })
  }

  navigateToJobSetForState(jobSet: string, jobState: string) {
    this.props.history.push({
      ...this.props.location,
      pathname: "/jobs",
      search: `queue=${this.state.queue}&job_set=${jobSet}&job_states=${jobState}`
    })
  }

  private fetchJobSets(queue: string): Promise<JobSet[]> {
    return this.props.jobService.getJobSets(queue)
  }

  private setStateAsync(state: JobSetsContainerState): Promise<void> {
    return new Promise(resolve => this.setState(state, resolve))
  }

  render() {
    return <JobSets
      queue={this.state.queue}
      jobSets={this.state.jobSets}
      onQueueChange={this.setQueue}
      onRefresh={this.refresh}
      onJobSetStatsClick={this.navigateToJobSetForState} />
  }
}

export default withRouter(JobSetsContainer)
