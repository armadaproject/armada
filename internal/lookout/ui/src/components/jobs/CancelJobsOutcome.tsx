import React, { Fragment, Ref } from "react"

import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Theme,
  colors,
  createStyles,
} from "@material-ui/core"
import { makeStyles } from "@material-ui/core/styles"

import { CancelJobsResult } from "../../services/JobService"
import LoadingButton from "./LoadingButton"

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    paper: {
      backgroundColor: theme.palette.background.paper,
      boxShadow: theme.shadows[5],
      padding: theme.spacing(2, 4, 3),
      outline: "none",
      borderRadius: "0.66em",
      maxHeight: "80%",
      maxWidth: "75%",
      display: "flex",
      flexDirection: "column",
      overflow: "hidden",
    },
    container: {
      minHeight: "6em",
      overflow: "auto",
      margin: theme.spacing(0, 0, 1),
    },
    successHeader: {
      backgroundColor: colors.green[100],
    },
    success: {
      backgroundColor: colors.green[50],
    },
    failureHeader: {
      backgroundColor: colors.red[100],
    },
    failure: {
      backgroundColor: colors.red[50],
    },
    button: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      marginTop: "1em",
    },
  }),
)

type CancelJobsOutcomeProps = {
  cancelJobsResult: CancelJobsResult
  isLoading: boolean
  onCancelJobs: () => void
}

const CancelJobsOutcome = React.forwardRef((props: CancelJobsOutcomeProps, ref: Ref<any>) => {
  const classes = useStyles()

  return (
    <div ref={ref} className={classes.paper}>
      <h2 id="cancel-jobs-modal-title" className="cancel-jobs-modal-title">
        Cancel jobs
      </h2>
      {props.cancelJobsResult.cancelledJobs.length > 0 && (
        <Fragment>
          <p id="cancel-jobs-modal-description" className="cancel-jobs-modal-description">
            The following jobs were cancelled successfully:
          </p>
          <TableContainer component={Paper} className={classes.container}>
            <Table stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell className={classes.successHeader}>Id</TableCell>
                  <TableCell className={classes.successHeader}>Job Set</TableCell>
                  <TableCell className={classes.successHeader}>Submission Time</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className={classes.success}>
                {props.cancelJobsResult.cancelledJobs.map((job) => (
                  <TableRow key={job.jobId}>
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
          <p id="cancel-jobs-modal-description" className="cancel-jobs-modal-description">
            The following jobs failed to cancel:
          </p>
          <TableContainer component={Paper} className={classes.container}>
            <Table stickyHeader>
              <TableHead className={classes.failureHeader}>
                <TableRow>
                  <TableCell className={classes.failureHeader}>Id</TableCell>
                  <TableCell className={classes.failureHeader}>Job Set</TableCell>
                  <TableCell className={classes.failureHeader}>State</TableCell>
                  <TableCell className={classes.failureHeader}>Submission Time</TableCell>
                  <TableCell className={classes.failureHeader}>Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className={classes.failure}>
                {props.cancelJobsResult.failedJobCancellations.map((failed) => (
                  <TableRow key={failed.job.jobId}>
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
          <div className={classes.button}>
            <LoadingButton content={"Retry"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
          </div>
        </Fragment>
      )}
    </div>
  )
})

export default CancelJobsOutcome
