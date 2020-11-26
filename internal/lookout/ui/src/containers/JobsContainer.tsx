import React from 'react'
import * as H from "history"
import { match, withRouter } from 'react-router-dom'
import queryString, { ParseOptions, StringifyOptions } from 'query-string'

import JobService, {
  JobInfoViewModel,
  JobStateViewModel,
  VALID_JOB_STATE_VIEW_MODELS
} from "../services/JobService"
import Jobs from "../components/Jobs"
import { updateArray } from "../utils";

type JobsContainerProps = {
  jobService: JobService
  history: H.History;
  location: H.Location;
  match: match;
}

interface JobsContainerState {
  jobInfos: JobInfoViewModel[]
  queue: string
  jobSet: string
  jobStates: JobStateViewModel[]
  newestFirst: boolean
  canLoadMore: boolean
}

type JobFiltersQueryParams = {
  queue?: string
  job_set?: string
  job_states?: JobStateViewModel[]
  newest_first?: boolean
}

const QUERY_STRING_OPTIONS: ParseOptions | StringifyOptions = {
  arrayFormat: "comma",
  parseBooleans: true
}

function setUrlParams(history: H.History, currentLocation: H.Location, state: JobsContainerState) {
  let queryObject: JobFiltersQueryParams = {}
  if (state.queue) {
    queryObject = {
      ...queryObject,
      queue: state.queue,
    }
  }
  if (state.jobSet) {
    queryObject = {
      ...queryObject,
      job_set: state.jobSet,
    }
  }
  if (state.jobStates) {
    queryObject = {
      ...queryObject,
      job_states: state.jobStates,
    }
  }
  if (state.newestFirst) {
    queryObject = {
      ...queryObject,
      newest_first: state.newestFirst,
    }
  }

  const query = queryString.stringify(queryObject, QUERY_STRING_OPTIONS)
  history.push({
    ...currentLocation,
    search: query,
  })
}

function parseUrlParams(history: H.History, currentLocation: H.Location): any {
  const params = queryString.parse(currentLocation.search, {
    arrayFormat: 'separator',
    arrayFormatSeparator: ','
  }) as JobFiltersQueryParams

  let filters = {}
  if (params.queue) {
    filters = {
      ...filters,
      queue: params.queue
    }
  }
  if (params.job_set) {
    filters = {
      ...filters,
      jobSet: params.job_set
    }
  }
  if (params.job_states) {
    filters = {
      ...filters,
      jobSet: params.job_set
    }
  }
  if (params.newest_first) {
    filters = {
      ...filters,
      newestFirst: params.newest_first
    }
  }

  return filters
}

class JobsContainer extends React.Component<JobsContainerProps, JobsContainerState> {
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

  componentDidMount() {
    const filters = parseUrlParams(this.props.history, this.props.location)
    this.setState({
      ...this.state,
      ...filters,
    }, () => {
      console.log(this.state)
    })
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
    }, () => {
      setUrlParams(this.props.history, this.props.location, this.state)
      callback()
    })
  }

  jobSetChange(jobSet: string, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      jobSet: jobSet,
    }, () => {
      setUrlParams(this.props.history, this.props.location, this.state)
      callback()
    })
  }

  jobStatesChange(jobStates: JobStateViewModel[], callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      jobStates: jobStates
    }, () => {
      setUrlParams(this.props.history, this.props.location, this.state)
      callback()
    })
  }

  orderChange(newestFirst: boolean, callback: () => void) {
    this.setState({
      ...this.state,
      jobInfos: [],
      canLoadMore: true,
      newestFirst: newestFirst
    }, () => {
      setUrlParams(this.props.history, this.props.location, this.state)
      callback()
    })
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

export default withRouter(JobsContainer)
