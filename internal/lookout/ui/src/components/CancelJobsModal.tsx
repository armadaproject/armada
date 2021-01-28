import React from "react"
import { Backdrop, createStyles, Fade, Modal, } from "@material-ui/core";
import { makeStyles } from "@material-ui/core/styles";

import { CancelJobsResult, Job } from "../services/JobService";
import { CancelJobsRequestStatus } from "../containers/JobsContainer";
import CancelJobs from "./CancelJobs";
import CancelJobsOutcome from "./CancelJobsOutcome";

export type CancelJobsModalState = "CancelJobs" | "CancelJobsResult" | "None"

export interface CancelJobsModalContext {
  modalState: CancelJobsModalState
  jobsToCancel: Job[]
  cancelJobsResult: CancelJobsResult
  cancelJobsRequestStatus: CancelJobsRequestStatus
}

interface CancelJobsDialogProps extends CancelJobsModalContext {
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
  const isOpen = props.modalState === "CancelJobs" || props.modalState === "CancelJobsResult"
  const isLoading = props.cancelJobsRequestStatus === "Loading"

  let content = <div/>
  if (props.modalState === "CancelJobs") {
    content = (
      <CancelJobs
        jobsToCancel={props.jobsToCancel}
        isLoading={isLoading}
        onCancelJobs={props.onCancelJobs}/>
    )
  }
  if (props.modalState === "CancelJobsResult") {
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
