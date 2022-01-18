import React, { Fragment } from "react"

import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@material-ui/core"

import { CancelJobsResponse } from "../../../services/JobService"
import LoadingButton from "../LoadingButton"

import "./CancelJobsOutcome.css"
import "../../Dialog.css"
import "../../Table.css"
import "../../Text.css"

type CancelJobsOutcomeProps = {
  cancelJobsResult: CancelJobsResponse
  isLoading: boolean
  onCancelJobs: () => void
}

export default function CancelJobsOutcome(props: CancelJobsOutcomeProps) {
  return (
    <div className="lookout-dialog-container">
      {props.cancelJobsResult.cancelledJobs.length > 0 && (
        <Fragment>
          <p className="lookout-dialog-fixed">The following jobs were cancelled successfully:</p>
          <TableContainer component={Paper} className="lookout-table-container lookout-dialog-varying">
            <Table stickyHeader className="lookout-table">
              <TableHead>
                <TableRow>
                  <TableCell className="success-header cancel-jobs-success-job-id-cell">Id</TableCell>
                  <TableCell className="success-header cancel-jobs-success-job-set-cell">Job Set</TableCell>
                  <TableCell className="success-header cancel-jobs-success-time-cell">Submission Time</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className="success">
                {props.cancelJobsResult.cancelledJobs.map((job) => (
                  <TableRow key={job.jobId} className="lookout-word-wrapped">
                    <TableCell>{job.jobId}</TableCell>
                    <TableCell>{job.jobSet}</TableCell>
                    <TableCell>{job.submissionTime}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </Fragment>
      )}
      {props.cancelJobsResult.failedJobCancellations.length > 0 && (
        <Fragment>
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
                {props.cancelJobsResult.failedJobCancellations.map((failed) => (
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
          <div className="lookout-dialog-fixed lookout-dialog-centered">
            <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
          </div>
        </Fragment>
      )}
    </div>
  )
}
