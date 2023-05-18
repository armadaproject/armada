import React from "react"

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Checkbox } from "@material-ui/core"

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
  isPlatformCancel: boolean
  setIsPlatformCancel: (x: boolean) => void
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
              <TableCell className="cancel-jobs-cell-job-id">Id</TableCell>
              <TableCell className="cancel-jobs-cell-job-set">Job Set</TableCell>
              <TableCell className="cancel-jobs-cell-state">State</TableCell>
              <TableCell className="cancel-jobs-cell-time">Submission Time</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobsToCancel.map((job) => (
              <TableRow key={job.jobId} className="lookout-word-wrapped">
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.jobSet}</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div>
        <label>Is Platform error?</label>
        <Checkbox
          checked={props.isPlatformCancel}
          disabled={props.isLoading}
          onChange={(event) => props.setIsPlatformCancel(event.target.checked)}
        />
      </div>
      <div className="lookout-dialog-fixed lookout-dialog-centered">
        <LoadingButton content={"Cancel Jobs"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
      </div>
    </div>
  )
}
