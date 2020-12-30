import React from "react";
import {
  Backdrop,
  createStyles,
  Modal,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
  Theme,
} from "@material-ui/core";

import { Job } from "../services/JobService";
import { makeStyles } from "@material-ui/core/styles";

type JobDetailModalProps = {
  title: string
  job: Job
  isOpen: boolean
  onClose: () => void
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
      padding: theme.spacing(2, 4, 3),
      outline: "none",
      borderRadius: "0.66em",
      maxHeight: "80%",
      maxWidth: "75%",
      display: "flex",
      flexDirection: "column",
    },
  }),
)

export default function JobDetailModal(props: JobDetailModalProps) {
  const classes = useStyles()

  return (
    <Modal
      aria-labelledby="cancel-jobs-modal-title"
      aria-describedby="cancel-jobs-modal-description"
      open={props.isOpen}
      className={classes.modal}
      closeAfterTransition
      BackdropComponent={Backdrop}
      BackdropProps={{
        timeout: 500,
      }}
      onClose={props.onClose}>
      <div className={classes.paper}>
        <h3 id="job-detail-modal-title">
          {props.title}
        </h3>
        <TableContainer component={Paper}>
          <Table stickyHeader>
            <TableBody>
              <TableRow>
                <TableCell>Id</TableCell>
                <TableCell>{props.job.jobId}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Job Set</TableCell>
                <TableCell>{props.job.jobSet}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Queue</TableCell>
                <TableCell>{props.job.queue}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Owner</TableCell>
                <TableCell>{props.job.owner}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell>Submission time</TableCell>
                <TableCell>{props.job.submissionTime}</TableCell>
              </TableRow>
              {props.job.cluster &&
                <TableRow>
                  <TableCell>Cluster</TableCell>
                  <TableCell>{props.job.cluster}</TableCell>
                </TableRow>
              }
            </TableBody>
          </Table>
        </TableContainer>
      </div>
    </Modal>
  )
}
