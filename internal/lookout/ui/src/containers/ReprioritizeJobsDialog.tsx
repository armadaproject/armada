import React, { useState } from "react"

import { Dialog, DialogContent, DialogTitle } from "@material-ui/core"

import ReprioritizeJobs from "../components/jobs/reprioritize-jobs/ReprioritizeJobs"
import ReprioritizeJobsOutcome from "../components/jobs/reprioritize-jobs/ReprioritizeJobsOutcome"
import JobService, { Job, ReprioritizeJobsResult } from "../services/JobService"
import { priorityIsValid, RequestStatus } from "../utils"
import { REPRIORITIZEABLE_JOB_STATES } from "./JobsContainer"

import "../components/Dialog.css"

export type ReprioritizeJobsDialogState = "ReprioritizeJobs" | "ReprioritizeJobsResult"
export type ReprioritizeJobsStatus = "Success" | "Failure" | "Partial success"

type ReprioritizeJobsProps = {
  isOpen: boolean
  selectedJobs: Job[]
  jobService: JobService
  onResult: (result: ReprioritizeJobsStatus) => void
  onClose: () => void
}

export default function ReprioritizeJobsDialog(props: ReprioritizeJobsProps) {
  const [state, setState] = useState<ReprioritizeJobsDialogState>("ReprioritizeJobs")
  const [result, setResult] = useState<ReprioritizeJobsResult>({
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
    const reprioritizeJobsResult = await props.jobService.reprioritizeJobs(jobsToReprioritize, Number(priority))
    setRequestStatus("Idle")

    setResult(reprioritizeJobsResult)
    setState("ReprioritizeJobsResult")
    if (reprioritizeJobsResult.failedJobReprioritizations.length === 0) {
      props.onResult("Success")
    } else if (reprioritizeJobsResult.reprioritizedJobs.length === 0) {
      props.onResult("Failure")
    } else {
      props.onResult("Partial success")
    }
  }

  function close() {
    props.onClose()
    setPriority("")
    setState("ReprioritizeJobs")
    setResult({
      reprioritizedJobs: [],
      failedJobReprioritizations: [],
    })
  }

  return (
    <Dialog
      open={props.isOpen}
      aria-labelledby="reprioritize-jobs-modal-title"
      aria-describedby="reprioritize-jobs-modal-description"
      onClose={close}
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
            reprioritizeJobsResult={result}
            isLoading={requestStatus === "Loading"}
            newPriority={priority}
            onReprioritizeJobs={reprioritizeJobs}
          />
        )}
      </DialogContent>
    </Dialog>
  )
}
