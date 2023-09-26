import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import { REPRIORITIZEABLE_JOB_STATES } from "./JobsContainer"
import ReprioritizeJobs from "../components/jobs/reprioritize-jobs/ReprioritizeJobs"
import ReprioritizeJobsOutcome from "../components/jobs/reprioritize-jobs/ReprioritizeJobsOutcome"
import { JobService, Job, ReprioritizeJobsResponse } from "../services/JobService"
import { ApiResult, priorityIsValid, RequestStatus } from "../utils"

import "../components/Dialog.css"

export type ReprioritizeJobsDialogState = "ReprioritizeJobs" | "ReprioritizeJobsResult"

type ReprioritizeJobsProps = {
  isOpen: boolean
  selectedJobs: Job[]
  jobService: JobService
  onResult: (result: ApiResult) => void
  onClose: () => void
}

export default function ReprioritizeJobsDialog(props: ReprioritizeJobsProps) {
  const [state, setState] = useState<ReprioritizeJobsDialogState>("ReprioritizeJobs")
  const [response, setResponse] = useState<ReprioritizeJobsResponse>({
    reprioritizedJobs: [],
    failedJobReprioritizations: [],
  })
  const [requestStatus, setRequestStatus] = useState<RequestStatus>("Idle")
  const [priority, setPriority] = useState<string>("")

  const jobsToReprioritize = props.selectedJobs.filter((job) => REPRIORITIZEABLE_JOB_STATES.includes(job.jobState))

  async function reprioritizeJobs() {
    if (requestStatus == "Loading" || !priorityIsValid(priority)) {
      return
    }

    setRequestStatus("Loading")
    const reprioritizeJobsResponse = await props.jobService.reprioritizeJobs(jobsToReprioritize, Number(priority))
    setRequestStatus("Idle")

    setResponse(reprioritizeJobsResponse)
    setState("ReprioritizeJobsResult")
    if (reprioritizeJobsResponse.failedJobReprioritizations.length === 0) {
      props.onResult("Success")
    } else if (reprioritizeJobsResponse.reprioritizedJobs.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function cleanup() {
    setPriority("")
    setState("ReprioritizeJobs")
    setResponse({
      reprioritizedJobs: [],
      failedJobReprioritizations: [],
    })
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="reprioritize-jobs-modal-title"
      aria-describedby="reprioritize-jobs-modal-description"
      onClose={props.onClose}
      TransitionProps={{
        onExited: cleanup,
      }}
      maxWidth={"md"}
    >
      <DialogTitle id="reprioritize-jobs-dialog-title">Reprioritize Jobs</DialogTitle>
      <DialogContent className="lookout-dialog">
        {state === "ReprioritizeJobs" && (
          <ReprioritizeJobs
            jobsToReprioritize={jobsToReprioritize}
            isLoading={requestStatus === "Loading"}
            newPriority={priority}
            isValid={priorityIsValid(priority)}
            onReprioritizeJobs={reprioritizeJobs}
            onPriorityChange={setPriority}
          />
        )}
        {state === "ReprioritizeJobsResult" && (
          <ReprioritizeJobsOutcome
            reprioritizeJobsResponse={response}
            isLoading={requestStatus === "Loading"}
            newPriority={priority}
            onReprioritizeJobs={reprioritizeJobs}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
