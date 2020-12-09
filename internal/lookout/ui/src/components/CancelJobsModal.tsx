import React from "react"
import {
  Backdrop, Button,
  createStyles,
  Modal, Paper,
  Table,
  TableBody,
  TableCell, TableContainer,
  TableHead,
  TableRow,
  Theme
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";
import { JobInfoViewModel } from "../services/JobService";

interface CancelJobsModalProps {
  open: boolean
  jobsToCancel: JobInfoViewModel[]
  onCancelJobs: () => void
  onBackdropClick: () => void
}

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    modal: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
    paper: {
      backgroundColor: theme.palette.background.paper,
      boxShadow: theme.shadows[5],
      padding: theme.spacing(2, 4, 2),
      outline: "none",
      borderRadius: "0.66em",
      maxHeight: "80%",
      maxWidth: "75%",
      display: "flex",
      flexDirection: "column",
    },
    container: {
      overflow: "auto",
    },
    button: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      marginTop: "1em",
    },
  }),
)

export default function CancelJobsModal(props: CancelJobsModalProps) {
  const classes = useStyles()
  return (
    <Modal
      aria-labelledby="cancel-jobs-modal-title"
      aria-describedby="cancel-jobs-modal-description"
      open={props.open}
      className={classes.modal}
      closeAfterTransition
      BackdropComponent={Backdrop}
      BackdropProps={{
        timeout: 500,
      }}>
      <div className={classes.paper}>
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
          <Table stickyHeader aria-label="simple-table">
            <TableHead>
              <TableRow>
                <TableCell>Id</TableCell>
                <TableCell>Set</TableCell>
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
          <Button
            color="secondary"
            onClick={props.onCancelJobs}>
            Cancel Jobs
          </Button>
        </div>
      </div>
    </Modal>
  )
}
