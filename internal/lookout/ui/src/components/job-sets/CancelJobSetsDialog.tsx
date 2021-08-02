import React from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import { CancelJobSetsResult, JobSet } from "../../services/JobService"
import { RequestStatus } from "../../utils"
import CancelJobSets from "./CancelJobSets"
import CancelJobSetsOutcome from "./CancelJobSetsOutcome"

export type CancelJobSetsDialogState = "CancelJobSets" | "CancelJobSetsResult" | "None"

export interface CancelJobSetsDialogContext {
  dialogState: CancelJobSetsDialogState
  queue: string
  jobSetsToCancel: JobSet[]
  cancelJobSetsResult: CancelJobSetsResult
  cancelJobSetsRequestStatus: RequestStatus
}

interface CancelJobSetsProps extends CancelJobSetsDialogContext {
  onCancelJobSets: () => void
  onClose: () => void
}

export default function CancelJobSetsDialog(props: CancelJobSetsProps) {
  const isOpen = props.dialogState === "CancelJobSets" || props.dialogState === "CancelJobSetsResult"
  const isLoading = props.cancelJobSetsRequestStatus === "Loading"

  let content = <div />
  if (props.dialogState === "CancelJobSets") {
    content = (
      <CancelJobSets
        queue={props.queue}
        jobSets={props.jobSetsToCancel}
        isLoading={isLoading}
        onCancelJobSets={props.onCancelJobSets}
      />
    )
  }
  if (props.dialogState === "CancelJobSetsResult") {
    content = (
      <CancelJobSetsOutcome
        cancelJobSetsResult={props.cancelJobSetsResult}
        isLoading={isLoading}
        onCancelJobs={props.onCancelJobSets}
      />
    )
  }

  return (
    <Dialog
      open={isOpen}
      aria-labelledby="cancel-job-sets-modal-title"
      aria-describedby="cancel-job-sets-modal-description"
      onClose={props.onClose}
      maxWidth={"md"}
    >
      <DialogTitle id="cancel-job-sets-dialog-title">Cancel Job Sets</DialogTitle>
      <DialogContent>{content}</DialogContent>
    </Dialog>
  )
}
