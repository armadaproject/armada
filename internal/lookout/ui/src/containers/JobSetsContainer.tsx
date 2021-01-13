import React from "react"
import { RouteComponentProps, withRouter } from "react-router-dom";

import JobService, { JobSet } from "../services/JobService";
import JobSets from "../components/JobSets";
import * as queryString from "querystring";

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
  const params = queryString.parse(query) as { queue: string }

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

    const jobSets = await this.props.jobService.getJobSets(queue)
    this.setState({
      queue: queue,
      jobSets: jobSets,
    })
  }

  async refresh() {
    const jobSets = await this.props.jobService.getJobSets(this.state.queue)
    this.setState({
      ...this.state,
      jobSets: jobSets,
    })
  }

  fetchJobSets(queue: string): Promise<JobSet[]> {
    return this.props.jobService.getJobSets(queue)
  }

  render() {
    return <JobSets
      queue={this.state.queue}
      jobSets={this.state.jobSets}
      onQueueChange={this.setQueue}
      onRefresh={this.refresh} />
  }
}

export default withRouter(JobSetsContainer)
