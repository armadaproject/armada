import React from "react"

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@material-ui/core"

import { Job } from "../../../services/JobService"
import LoadingButton from "../LoadingButton"

import "./CancelJobs.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type CancelJobsProps = {
  jobsToCancel: Job[]
  isLoading: boolean
  onCancelJobs: () => void
}

export default function CancelJobs(props: CancelJobsProps) {
  return (
    <div className="lookout-dialog-container">
      <p id="cancel-jobs-description" className="lookout-dialog-fixed">
        The following jobs will be cancelled:
      </p>
      <TableContainer component={Paper} className="lookout-table-container lookout-dialog-varying">
        <Table stickyHeader className="lookout-table">
          <TableHead>
            <TableRow>
              <TableCell className="cancel-jobs-job-id-cell">Id</TableCell>
              <TableCell className="cancel-jobs-job-set-cell">Job Set</TableCell>
              <TableCell className="cancel-jobs-state-cell">State</TableCell>
              <TableCell className="cancel-jobs-time-cell">Submission Time</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobsToCancel.map((job) => (
              <TableRow key={job.jobId} className="lookout-word-wrapped">
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.jobSet}cancel-jobs</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div className="lookout-dialog-fixed lookout-dialog-centered">
        <LoadingButton content={"Cancel Jobs"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
      </div>
    </div>
  )
}
