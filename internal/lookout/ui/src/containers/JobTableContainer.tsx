import React from 'react';

import { JobService } from "../services/jobs";
import JobTableComponent from "../components/JobTableComponent";
import { LookoutJobInfo } from "../openapi/models";

type JobTableContainerProps = {
  jobService: JobService
}

type JobTableContainerState = {
  jobs: LookoutJobInfo[]
}

export class JobTableContainer extends React.Component<JobTableContainerProps, JobTableContainerState> {
  constructor(props: JobTableContainerProps) {
    super(props);
    this.state = { jobs: [] }
  }

  render() {
    return <JobTableComponent fetchJobs={(start, stop) => this.props.jobService.getJobsInQueue("test", start, stop)} isLoaded={() => true}/>
  }
}
