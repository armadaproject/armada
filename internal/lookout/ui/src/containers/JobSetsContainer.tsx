import React from "react"
import { RouteComponentProps, withRouter } from "react-router-dom";
import * as queryString from "querystring";

import JobSets from "../components/job-sets/JobSets";
import JobService, { JobSet } from "../services/JobService";
import { debounced } from "../utils";

type JobSetsContainerProps = {
  jobService: JobService
} & RouteComponentProps

type JobSetsContainerParams = {
  queue: string
  currentView: JobSetsView
}

type JobSetsContainerState = {
  jobSets: JobSet[]
} & JobSetsContainerParams

export type JobSetsView = "job-counts" | "runtime" | "queued-time"

type JobSetsQueryParams = {
  queue?: string
  view?: string
}

export function isJobSetsView(val: string): val is JobSetsView {
  return ["job-counts", "runtime", "queued-time"].includes(val)
}

function makeQueryString(queue: string, view: JobSetsView): string {
  const queryObject: JobSetsQueryParams = {}

  if (queue) {
    queryObject.queue = queue
  }
  queryObject.view = view

  return queryString.stringify(queryObject)
}

function getParamsFromQueryString(query: string): JobSetsContainerParams {
  if (query[0] === "?") {
    query = query.slice(1)
  }
  const params = queryString.parse(query) as JobSetsQueryParams

  return {
    queue: params.queue ?? "",
    currentView: params.view && isJobSetsView(params.view) ? params.view : "job-counts",
  }
}

class JobSetsContainer extends React.Component<JobSetsContainerProps, JobSetsContainerState> {
  constructor(props: JobSetsContainerProps) {
    super(props)

    this.state ={
      queue: "",
      jobSets: [],
      currentView: "job-counts",
    }

    this.setQueue = this.setQueue.bind(this)
    this.setView = this.setView.bind(this)
    this.refresh = this.refresh.bind(this)
    this.navigateToJobSetForState = this.navigateToJobSetForState.bind(this)

    this.fetchJobSets = debounced(this.fetchJobSets.bind(this), 100)
  }

  async componentDidMount() {
    const params = getParamsFromQueryString(this.props.location.search)
    await this.setStateAsync({
      ...this.state,
      ...params,
    })

    const jobSets = await this.fetchJobSets(params.queue)
    this.setState({
      ...this.state,
      jobSets: jobSets,
    })
  }

  async setQueue(queue: string) {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(queue, this.state.currentView),
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

  setView(view: JobSetsView) {
    this.props.history.push({
      ...this.props.location,
      search: makeQueryString(this.state.queue, view),
    })

    this.setState({
      ...this.state,
      currentView: view,
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
      view={this.state.currentView}
      jobSets={this.state.jobSets}
      onQueueChange={this.setQueue}
      onViewChange={this.setView}
      onRefresh={this.refresh}
      onJobSetClick={this.navigateToJobSetForState} />
  }
}

export default withRouter(JobSetsContainer)
