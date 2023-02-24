import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import CancelJobs from "../components/jobs/cancel-jobs/CancelJobs"
import CancelJobsOutcome from "../components/jobs/cancel-jobs/CancelJobsOutcome"
import { JobService, CancelJobsResponse, Job } from "../services/JobService"
import { ApiResult, RequestStatus } from "../utils"
import { CANCELLABLE_JOB_STATES } from "./JobsContainer"

import "../components/Dialog.css"

export type CancelJobsDialogState = "CancelJobs" | "CancelJobsResult"

type CancelJobsProps = {
  isOpen: boolean
  selectedJobs: Job[]
  jobService: JobService
  onResult: (result: ApiResult) => void
  onClose: () => void
}

export default function CancelJobsDialog(props: CancelJobsProps) {
  const [state, setState] = useState<CancelJobsDialogState>("CancelJobs")
  const [response, setResponse] = useState<CancelJobsResponse>({
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
    const cancelJobsResponse = await props.jobService.cancelJobs(jobsToCancel)
    setRequestStatus("Idle")

    setResponse(cancelJobsResponse)
    setState("CancelJobsResult")
    if (cancelJobsResponse.failedJobCancellations.length === 0) {
      props.onResult("Success")
    } else if (cancelJobsResponse.cancelledJobs.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function cleanup() {
    setState("CancelJobs")
    setResponse({
      cancelledJobs: [],
      failedJobCancellations: [],
    })
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="cancel-jobs-modal-title"
      aria-describedby="cancel-jobs-modal-description"
      onClose={props.onClose}
      TransitionProps={{
        onExited: cleanup,
      }}
      maxWidth={"md"}
    >
      <DialogTitle id="cancel-jobs-dialog-title">Cancel Jobs</DialogTitle>
      <DialogContent className="lookout-dialog">
        {state === "CancelJobs" && (
          <CancelJobs jobsToCancel={jobsToCancel} isLoading={requestStatus == "Loading"} onCancelJobs={cancelJobs} />
        )}
        {state === "CancelJobsResult" && (
          <CancelJobsOutcome
            cancelJobsResponse={response}
            isLoading={requestStatus == "Loading"}
            onCancelJobs={cancelJobs}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
