import { useEffect, useState } from "react"

import { Job } from "../models/lookoutV2Models"
import { IGetJobSpecService } from "../services/lookoutV2/GetJobSpecService"
import { getErrorMessage, RequestStatus } from "../utils"
import { OpenSnackbarFn } from "./useCustomSnackbar"

export type JobSpecState = {
  jobSpec?: Record<string, any>
  loadState: RequestStatus
}

export const useJobSpec = (
  job: Job,
  jobSpecService: IGetJobSpecService,
  openSnackbar: OpenSnackbarFn,
): JobSpecState => {
  const [jobSpecState, setJobSpecState] = useState<JobSpecState>({
    loadState: "Idle",
  })
  useEffect(() => {
    const loadJobSpec = async () => {
      setJobSpecState({
        loadState: "Loading",
      })
      try {
        const jobSpec = await jobSpecService.getJobSpec(job.jobId, undefined)
        setJobSpecState({
          jobSpec: jobSpec,
          loadState: "Idle",
        })
      } catch (e) {
        const errMsg = await getErrorMessage(e)
        console.error(errMsg)
        openSnackbar("Failed to retrieve Job spec for Job with ID: " + job.jobId + ": " + errMsg, "error")
        setJobSpecState({
          ...jobSpecState,
          loadState: "Idle",
        })
      }
    }
    loadJobSpec()
  }, [job])
  return jobSpecState
}
