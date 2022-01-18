import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import CancelJobs from "../components/jobs/cancel-jobs/CancelJobs"
import CancelJobsOutcome from "../components/jobs/cancel-jobs/CancelJobsOutcome"
import JobService, { CancelJobsResponse, Job } from "../services/JobService"
import { RequestStatus } from "../utils"
import { CANCELLABLE_JOB_STATES } from "./JobsContainer"

import "../components/Dialog.css"

export type CancelJobsDialogState = "CancelJobs" | "CancelJobsResult"
export type CancelJobsStatus = "Success" | "Failure" | "Partial success"

type CancelJobsProps = {
  isOpen: boolean
  selectedJobs: Job[]
  jobService: JobService
  onResult: (result: CancelJobsStatus) => void
  onClose: () => void
}

export default function CancelJobsDialog(props: CancelJobsProps) {
  const [state, setState] = useState<CancelJobsDialogState>("CancelJobs")
  const [result, setResult] = useState<CancelJobsResponse>({
    cancelledJobs: [],
    failedJobCancellations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")

  const jobsToCancel = props.selectedJobs.filter((job) => CANCELLABLE_JOB_STATES.includes(job.jobState))

  async function cancelJobs() {
    if (requestStatus === "Loading") {
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
    props.onClose()
    setState("CancelJobs")
    setResult({
      cancelledJobs: [],
      failedJobCancellations: [],
    })
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
      <DialogContent className="lookout-dialog">
        {state === "CancelJobs" && (
          <CancelJobs jobsToCancel={jobsToCancel} isLoading={requestStatus == "Loading"} onCancelJobs={cancelJobs} />
        )}
        {state === "CancelJobsResult" && (
          <CancelJobsOutcome
            cancelJobsResult={result}
            isLoading={requestStatus == "Loading"}
            onCancelJobs={cancelJobs}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
