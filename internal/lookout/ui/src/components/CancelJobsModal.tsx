import React from "react"
import {
  Backdrop,
  createStyles,
  Fade,
  Modal,
} from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";

import { CancelJobsResult, Job } from "../services/JobService";
import { CancelJobsRequestStatus, ModalState } from "../containers/JobsContainer";
import CancelJobs from "./CancelJobs";
import CancelJobsOutcome from "./CancelJobsOutcome";

interface CancelJobsDialogProps {
  currentOpenModal: ModalState
  jobsToCancel: Job[]
  cancelJobsResult: CancelJobsResult
  cancelJobsRequestStatus: CancelJobsRequestStatus
  onCancelJobs: () => void
  onClose: () => void
}

const useStyles = makeStyles(() =>
  createStyles({
    modal: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    }
  }),
)

export default function CancelJobsModal(props: CancelJobsDialogProps) {
  const classes = useStyles()
  const isOpen = props.currentOpenModal === "CancelJobs" || props.currentOpenModal === "CancelJobsResult"
  const isLoading = props.cancelJobsRequestStatus === "Loading"

  let content = <div/>
  if (props.currentOpenModal === "CancelJobs") {
    content = (
      <CancelJobs
        jobsToCancel={props.jobsToCancel}
        isLoading={isLoading}
        onCancelJobs={props.onCancelJobs}/>
    )
  }
  if (props.currentOpenModal === "CancelJobsResult") {
    content = (
      <CancelJobsOutcome
        cancelJobsResult={props.cancelJobsResult}
        isLoading={isLoading}
        onCancelJobs={props.onCancelJobs}/>
    )
  }

  return (
    <Modal
      aria-labelledby="cancel-jobs-modal-title"
      aria-describedby="cancel-jobs-modal-description"
      open={isOpen}
      className={classes.modal}
      closeAfterTransition
      BackdropComponent={Backdrop}
      BackdropProps={{
        timeout: 500,
      }}
      onClose={props.onClose}>
      <>
        <Fade in={isOpen}>
          {content}
        </Fade>
      </>
    </Modal>
  )
}
