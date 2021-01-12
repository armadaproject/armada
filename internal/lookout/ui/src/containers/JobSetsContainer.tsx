import React from "react"
import { RouteComponentProps, withRouter } from "react-router-dom";

import JobService, { JobSet } from "../services/JobService";

type JobSetsContainerProps = {
  jobService: JobService
} & RouteComponentProps

interface JobSetsContainerState {
  jobSets: JobSet[]
}

class JobSetsContainer extends React.Component<JobSetsContainerProps, JobSetsContainerState> {
  constructor(props: JobSetsContainerProps) {
    super(props)
  }

  render() {
    return <h1>Job Sets</h1>
  }
}

export default withRouter(JobSetsContainer)
