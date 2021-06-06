import React from "react"

import { Backdrop, Fade, createStyles, Modal } from "@material-ui/core"
import { makeStyles } from "@material-ui/core/styles"

import { ReprioritizeJobsRequestStatus } from "../../containers/JobsContainer"
import { ReprioritizeJobsResult, Job } from "../../services/JobService"
import ReprioritizeJobs from "./ReprioritizeJobs"
import ReprioritizeJobsOutcome from "./ReprioritizeJobsOutcome"

export type ReprioritizeJobsModalState = "ReprioritizeJobs" | "ReprioritizeJobsResult" | "None"

export interface ReprioritizeJobsModalContext {
  modalState: ReprioritizeJobsModalState
  newPriority: number
  isValid: boolean
  jobsToReprioritize: Job[]
  reprioritizeJobsResult: ReprioritizeJobsResult
  reprioritizeJobsRequestStatus: ReprioritizeJobsRequestStatus
}

interface ReprioritizeJobsDialogProps extends ReprioritizeJobsModalContext {
  onReprioritizeJobs: () => void
  onPriorityChange: (e: any) => void
  onClose: () => void
}

const useStyles = makeStyles(() =>
  createStyles({
    modal: {
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
    },
  }),
)

export default function ReprioritizeJobsModal(props: ReprioritizeJobsDialogProps) {
  const classes = useStyles()
  const isOpen = props.modalState === "ReprioritizeJobs" || props.modalState === "ReprioritizeJobsResult"
  const isLoading = props.reprioritizeJobsRequestStatus === "Loading"

  let content = <div />
  if (props.modalState === "ReprioritizeJobs") {
    content = (
      <ReprioritizeJobs
        jobsToReprioritize={props.jobsToReprioritize}
        isLoading={isLoading}
        isValid={props.isValid}
        onReprioritizeJobs={props.onReprioritizeJobs}
        onPriorityChange={props.onPriorityChange}
      />
    )
  }
  if (props.modalState === "ReprioritizeJobsResult") {
    content = (
      <ReprioritizeJobsOutcome
        reprioritizeJobsResult={props.reprioritizeJobsResult}
        isLoading={isLoading}
        newPriority={props.newPriority}
        onReprioritizeJobs={props.onReprioritizeJobs}
      />
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
      onClose={props.onClose}
    >
      <>
        <Fade in={isOpen}>{content}</Fade>
      </>
    </Modal>
  )
}
