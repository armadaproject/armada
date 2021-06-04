import React, { Ref } from "react"

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

import { Job } from "../../services/JobService"
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

type ReprioritizeJobsProps = {
  jobsToReprioritize: Job[]
  isLoading: boolean
  onReprioritizeJobs: () => void
}

const ReprioritizeJobs = React.forwardRef((props: ReprioritizeJobsProps, ref: Ref<any>) => {
  const classes = useStyles()

  return (
    <div ref={ref} className={classes.paper}>
      <h2 id="cancel-jobs-modal-title" className="cancel-jobs-modal-title">
        Reprioritize jobs
      </h2>
      <p id="cancel-jobs-modal-description" className="cancel-jobs-modal-description">
        The following jobs will be reprioritized:
      </p>
      <TableContainer component={Paper} className={classes.container}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Id</TableCell>
              <TableCell>Job Set</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Submission Time</TableCell>
              <TableCell>Current Priority</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobsToReprioritize.map((job) => (
              <TableRow key={job.jobId}>
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.jobSet}</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
                <TableCell>{job.priority}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div className={classes.button}>
        <LoadingButton content={"Reprioritize Jobs"} isLoading={props.isLoading} onClick={props.onReprioritizeJobs} />
      </div>
    </div>
  )
})

export default ReprioritizeJobs
