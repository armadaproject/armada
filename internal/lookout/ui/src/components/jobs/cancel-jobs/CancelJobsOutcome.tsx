import React from "react"

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Checkbox } from "@material-ui/core"

import { CancelJobsResponse } from "../../../services/JobService"
import LoadingButton from "../LoadingButton"

import "./CancelJobsOutcome.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type CancelJobsOutcomeProps = {
  cancelJobsResponse: CancelJobsResponse
  isLoading: boolean
  onCancelJobs: () => void
  isPlatformCancel: boolean
  setIsPlatformCancel: (x: boolean) => void
}

export default function CancelJobsOutcome(props: CancelJobsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {props.cancelJobsResponse.cancelledJobs.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following jobs were cancelled successfully:</p>
          <TableContainer component={Paper} className="lookout-table-container lookout-dialog-varying">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="success-header cancel-jobs-cell-success-job-id">Id</TableCell>
                  <TableCell className="success-header cancel-jobs-cell-success-job-set">Job Set</TableCell>
                  <TableCell className="success-header cancel-jobs-cell-success-time">Submission Time</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="success">
                {props.cancelJobsResponse.cancelledJobs.map((job) => (
                  <TableRow key={job.jobId} className="lookout-word-wrapped">
                    <TableCell>{job.jobId}</TableCell>
                    <TableCell>{job.jobSet}</TableCell>
                    <TableCell>{job.submissionTime}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </>
      )}
      {props.cancelJobsResponse.failedJobCancellations.length > 0 && (
        <>
          <p className="lookout-dialog-fixed">The following jobs failed to cancel:</p>
          <TableContainer component={Paper} className="lookout-table-container lookout-dialog-varying">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="failure-header cancel-jobs-cell-failure-job-id">Id</TableCell>
                  <TableCell className="failure-header cancel-jobs-cell-failure-job-set">Job Set</TableCell>
                  <TableCell className="failure-header cancel-jobs-cell-failure-state">State</TableCell>
                  <TableCell className="failure-header cancel-jobs-cell-failure-time">Submission Time</TableCell>
                  <TableCell className="failure-header cancel-jobs-cell-failure-error">Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="failure">
                {props.cancelJobsResponse.failedJobCancellations.map((failed) => (
                  <TableRow key={failed.job.jobId} className="lookout-word-wrapped">
                    <TableCell>{failed.job.jobId}</TableCell>
                    <TableCell>{failed.job.jobSet}</TableCell>
                    <TableCell>{failed.job.jobState}</TableCell>
                    <TableCell>{failed.job.submissionTime}</TableCell>
                    <TableCell>{failed.error}</TableCell>
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
            <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
          </div>
        </>
      )}
    </div>
  )
}
