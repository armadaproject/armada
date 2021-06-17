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
  TextField,
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
    input: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
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
  isValid: boolean
  onReprioritizeJobs: () => void
  onPriorityChange: (e: string) => void
}

const ReprioritizeJobs = React.forwardRef((props: ReprioritizeJobsProps, ref: Ref<any>) => {
  const classes = useStyles()

  return (
    <div ref={ref} className={classes.paper}>
      <h2 id="reprioritize-jobs-modal-title" className="reprioritize-jobs-modal-title">
        Reprioritize jobs
      </h2>
      <p id="reprioritize-jobs-modal-description" className="reprioritize-jobs-modal-description">
        The following jobs will be reprioritized:
      </p>
      <TableContainer component={Paper} className={classes.container}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Id</TableCell>
              <TableCell>Job Set</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Current Priority</TableCell>
              <TableCell>Submission Time</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobsToReprioritize.map((job) => (
              <TableRow key={job.jobId}>
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.jobSet}</TableCell>
                <TableCell>{job.jobState}</TableCell>
                <TableCell>{job.priority}</TableCell>
                <TableCell>{job.submissionTime}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      <div className={classes.input}>
        <TextField
          autoFocus={true}
          placeholder={"New priority"}
          margin={"normal"}
          type={"text"}
          error={!props.isValid}
          helperText={!props.isValid ? "Value must be a number >= 0" : " "}
          onChange={(event) => props.onPriorityChange(event.target.value)}
        />
      </div>
      <div className={classes.button}>
        <LoadingButton
          content={"Reprioritize Jobs"}
          isDisabled={!props.isValid}
          isLoading={props.isLoading}
          onClick={props.onReprioritizeJobs}
        />
      </div>
    </div>
  )
})

export default ReprioritizeJobs
