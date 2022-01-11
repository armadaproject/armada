import React from "react"

import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Theme,
  createStyles,
} from "@material-ui/core"
import { makeStyles } from "@material-ui/core/styles"

import { Job } from "../../../services/JobService"
import LoadingButton from "../LoadingButton"

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    paper: {
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

export default function CancelJobs(props: CancelJobsProps) {
  const classes = useStyles()

  return (
    <div className={classes.paper}>
      <p id="cancel-jobs-modal-description" className="cancel-jobs-modal-description">
        The following jobs will be cancelled:
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
                <TableCell>{job.jobSet}cancel-jobs</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div className={classes.button}>
        <LoadingButton content={"Cancel Jobs"} isLoading={props.isLoading} onClick={props.onCancelJobs} />
      </div>
    </div>
  )
}
