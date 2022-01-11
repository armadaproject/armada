import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import { CANCELLABLE_JOB_STATES } from "../../../containers/JobsContainer"
import JobService, { CancelJobsResult, Job } from "../../../services/JobService"
import { RequestStatus } from "../../../utils"
import CancelJobs from "./CancelJobs"
import CancelJobsOutcome from "./CancelJobsOutcome"

export type CancelJobsDialogState = "CancelJobs" | "CancelJobsResult"
export type CancelJobsStatus = "Success" | "Failure" | "Partial success"

type CancelJobsProps = {
  isOpen: boolean
  selectedJobs: Job[]
  jobService: JobService
  onResult(result: CancelJobsStatus): void
  onClose(): void
}

export default function CancelJobsDialog(props: CancelJobsProps) {
  const [state, setState] = useState<CancelJobsDialogState>("CancelJobs")
  const [result, setResult] = useState<CancelJobsResult>({
    cancelledJobs: [],
    failedJobCancellations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")

  const jobsToCancel = props.selectedJobs.filter((job) => CANCELLABLE_JOB_STATES.includes(job.jobState))

  async function cancelJobs() {
    if (requestStatus == "Loading") {
      return
    }

    setRequestStatus("Loading")
    const cancelJobsResult = await props.jobService.cancelJobs(jobsToCancel)
    setRequestStatus("Idle")

    setResult(cancelJobsResult)
    setState("CancelJobsResult")
    if (cancelJobsResult.failedJobCancellations.length === 0) {
      props.onResult("Success")
    } else if (cancelJobsResult.cancelledJobs.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function close() {
    setState("CancelJobs")
    setResult({
      cancelledJobs: [],
      failedJobCancellations: [],
    })
    props.onClose()
  }

  let content: JSX.Element
  switch (state) {
    case "CancelJobs":
      content = (
        <CancelJobs jobsToCancel={jobsToCancel} isLoading={requestStatus == "Loading"} onCancelJobs={cancelJobs} />
      )
      break
    case "CancelJobsResult":
      content = (
        <CancelJobsOutcome cancelJobsResult={result} isLoading={requestStatus == "Loading"} onCancelJobs={cancelJobs} />
      )
      break
    default:
      content = <div />
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="cancel-jobs-modal-title"
      aria-describedby="cancel-jobs-modal-description"
      onClose={close}
      maxWidth={"md"}
    >
      <DialogTitle id="cancel-jobs-dialog-title">Cancel Jobs</DialogTitle>
      <DialogContent>{content}</DialogContent>
    </Dialog>
  )
}
