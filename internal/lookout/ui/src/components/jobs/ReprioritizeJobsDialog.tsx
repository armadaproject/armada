import React from "react"

import { Dialog, DialogTitle, DialogContent } from "@material-ui/core"

import { ReprioritizeJobsResult, Job } from "../../services/JobService"
import { RequestStatus } from "../../utils"
import ReprioritizeJobs from "./ReprioritizeJobs"
import ReprioritizeJobsOutcome from "./ReprioritizeJobsOutcome"

export type ReprioritizeJobsDialogState = "ReprioritizeJobs" | "ReprioritizeJobsResult" | "None"

export interface ReprioritizeJobsDialogContext {
  modalState: ReprioritizeJobsDialogState
  newPriority: number
  isValid: boolean
  jobsToReprioritize: Job[]
  reprioritizeJobsResult: ReprioritizeJobsResult
  reprioritizeJobsRequestStatus: RequestStatus
}

interface ReprioritizeJobsDialogProps extends ReprioritizeJobsDialogContext {
  onReprioritizeJobs: () => void
  onPriorityChange: (e: string) => void
  onClose: () => void
}

export default function ReprioritizeJobsDialog(props: ReprioritizeJobsDialogProps) {
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
    <Dialog
      open={isOpen}
      aria-labelledby="reprioritize-jobs-dialog-title"
      aria-describedby="reprioritize-jobs-dialog-description"
      onClose={props.onClose}
      maxWidth={"md"}
    >
      <DialogTitle id="reprioritize-jobs-dialog-title">Reprioritize Jobs</DialogTitle>
      <DialogContent>{content}</DialogContent>
    </Dialog>
  )
}
