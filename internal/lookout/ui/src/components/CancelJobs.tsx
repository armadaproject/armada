import React, { Ref } from "react";
import {
  createStyles,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow, Theme
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";

import { Job } from "../services/JobService";
import LoadingButton from "./LoadingButton";

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
    },
    container: {
      overflow: "auto",
      margin: theme.spacing(0, 0, 1),
    },
    button: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      marginTop: "1em",
    },
  }),
)

type CancelJobsProps = {
  jobsToCancel: Job[]
  isLoading: boolean
  onCancelJobs: () => void
}

const CancelJobs = React.forwardRef((props: CancelJobsProps, ref: Ref<any>) => {
  const classes = useStyles()

  return (
    <div ref={ref} className={classes.paper}>
      <h2
        id="cancel-jobs-modal-title"
        className="cancel-jobs-modal-title">
        Cancel jobs
      </h2>
      <p
        id="cancel-jobs-modal-description"
        className="cancel-jobs-modal-description">
        Cancel the following jobs?
      </p>
      <TableContainer component={Paper} className={classes.container}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Id</TableCell>
              <TableCell>Job Set</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Submission Time</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobsToCancel.map((job) => (
              <TableRow key={job.jobId}>
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.jobSet}</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div className={classes.button}>
        <LoadingButton
          content={"Cancel Jobs"}
          isLoading={props.isLoading}
          onClick={props.onCancelJobs} />
      </div>
    </div>
  )
})

export default CancelJobs
