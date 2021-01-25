import React from 'react'
import { RouteComponentProps, withRouter } from "react-router-dom";

import JobService, { Job } from "../services/JobService";
import JobDetails from "../components/JobDetails";
import { debounced } from "../utils";
import queryString from "querystring";

type JobDetailsContainerProps = {
  jobService: JobService
} & RouteComponentProps

interface JobDetailsContainerState {
  jobId: string
  job?: Job
  expandedItems: Set<string>
}

function makeQueryString(jobId: string): string {
  if (!jobId) {
    return ""
  }

  const queryObject = {
    id: jobId
  }

  return queryString.stringify(queryObject)
}

function getJobIdFromQueryString(query: string): string {
  if (query[0] === "?") {
    query = query.slice(1)
  }
  const params = queryString.parse(query) as { id?: string }

  return params.id ?? ""
}

class JobDetailsContainer extends React.Component<JobDetailsContainerProps, JobDetailsContainerState> {
  constructor(props: JobDetailsContainerProps) {
    super(props)

    this.state = {
      jobId: "",
      job: undefined,
      expandedItems: new Set(),
    }

    this.setJobId = this.setJobId.bind(this)
    this.refresh = this.refresh.bind(this)
    this.toggleExpanded = this.toggleExpanded.bind(this)

    this.fetchJob = debounced(this.fetchJob.bind(this), 100)
  }

  async componentDidMount() {
    const jobId = getJobIdFromQueryString(this.props.location.search)
    const job = await this.fetchJob(jobId)

    this.setState({
      jobId: jobId,
      job: job,
    })
  }

  async setJobId(jobId: string) {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(jobId),
    })

    await this.setStateAsync({
      ...this.state,
      jobId: jobId,
    })

    // Performed separately because debounced
    const job = await this.fetchJob(jobId)
    this.setState({
      ...this.state,
      job: job,
    })
  }

  async refresh() {
    const job = await this.fetchJob(this.state.jobId)
    this.setState({
      ...this.state,
      job: job,
    })
  }

  toggleExpanded(item: string, isExpanded: boolean) {
    const newExpanded = new Set(this.state.expandedItems)
    if (isExpanded) {
      newExpanded.add(item)
    } else {
      newExpanded.delete(item)
    }

    this.setState({
      ...this.state,
      expandedItems: newExpanded,
    })
  }

  private fetchJob(jobId: string): Promise<Job | undefined> {
    return this.props.jobService.getJob(jobId)
  }

  private setStateAsync(state: JobDetailsContainerState): Promise<void> {
    return new Promise(resolve => this.setState(state, resolve))
  }

  render() {
    return <JobDetails
      jobId={this.state.jobId}
      job={this.state.job}
      expandedItems={this.state.expandedItems}
      onJobIdChange={this.setJobId}
      onRefresh={this.refresh}
      onToggleExpand={this.toggleExpanded} />
  }
}

export default withRouter(JobDetailsContainer)
