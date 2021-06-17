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

import { ReprioritizeJobsResult } from "../../services/JobService"
import LoadingButton from "./LoadingButton"

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    paper: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(1, 4, 3),
      outline: "none",
      borderRadius: "0.66em",
      maxHeight: "100%",
      maxWidth: "100%",
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

type ReprioritizeJobsOutcomeProps = {
  reprioritizeJobsResult: ReprioritizeJobsResult
  newPriority: number
  isLoading: boolean
  onReprioritizeJobs: () => void
}

const ReprioritizeJobsOutcome = React.forwardRef((props: ReprioritizeJobsOutcomeProps, ref: Ref<any>) => {
  const classes = useStyles()

  return (
    <div ref={ref} className={classes.paper}>
      {props.reprioritizeJobsResult.reprioritizedJobs.length > 0 && (
        <Fragment>
          <p id="reprioritize-jobs-modal-description" className="reprioritize-jobs-modal-description">
            The following jobs were reprioritized successfully:
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
                {props.reprioritizeJobsResult.reprioritizedJobs.map((job) => (
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
      {props.reprioritizeJobsResult.failedJobReprioritizations.length > 0 && (
        <Fragment>
          <p id="reprioritize-jobs-modal-description" className="reprioritize-jobs-modal-description">
            Failed to reprioritize the following jobs:
          </p>
          <TableContainer component={Paper} className={classes.container}>
            <Table stickyHeader>
              <TableHead className={classes.failureHeader}>
                <TableRow>
                  <TableCell className={classes.failureHeader}>Id</TableCell>
                  <TableCell className={classes.failureHeader}>Job Set</TableCell>
                  <TableCell className={classes.failureHeader}>Current Priority</TableCell>
                  <TableCell className={classes.failureHeader}>Submission Time</TableCell>
                  <TableCell className={classes.failureHeader}>Error</TableCell>
                </TableRow>
              </TableHead>
              <TableBody className={classes.failure}>
                {props.reprioritizeJobsResult.failedJobReprioritizations.map((failed) => (
                  <TableRow key={failed.job.jobId}>
                    <TableCell>{failed.job.jobId}</TableCell>
                    <TableCell>{failed.job.jobSet}</TableCell>
                    <TableCell>{failed.job.priority}</TableCell>
                    <TableCell>{failed.job.submissionTime}</TableCell>
                    <TableCell>{failed.error}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <div className={classes.button}>
            <LoadingButton
              content={"Retry - New priority " + props.newPriority}
              isLoading={props.isLoading}
              onClick={props.onReprioritizeJobs}
            />
          </div>
        </Fragment>
      )}
    </div>
  )
})

export default ReprioritizeJobsOutcome
