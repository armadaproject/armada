import React, { useCallback, useEffect, useMemo, useRef, useState } from "react"

import { ContentCopy, Download } from "@mui/icons-material"
import { IconButton } from "@mui/material"
import yaml from "js-yaml"
import { Job } from "models/lookoutV2Models"

import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { getErrorMessage } from "../../../utils"
import styles from "./SidebarTabJobYaml.module.css"

export interface SidebarTabJobRuns {
  job: Job
  jobSpecService: IGetJobSpecService
}

type LoadState = "Idle" | "Loading"

type JobSpecState = {
  jobSpec?: Record<string, any>
  loadState: LoadState
}

function toJobSubmissionYaml(jobSpec: Record<string, any>): string {
  const submission: Record<string, any> = {}
  submission.queue = jobSpec.queue
  submission.jobSetId = jobSpec.jobSetId

  const job: Record<string, any> = {}
  job.priority = jobSpec.priority
  job.namespace = jobSpec.namespace
  job.annotations = jobSpec.annotations
  if (jobSpec.podSpec !== undefined) {
    job.podSpecs = [jobSpec.podSpec]
  }
  if (jobSpec.podSpecs !== undefined && Array.isArray(jobSpec.podSpecs) && jobSpec.podSpecs.length > 0) {
    job.podSpecs = jobSpec.podSpecs
  }
  job.ingress = jobSpec.ingress
  job.services = jobSpec.services
  job.scheduler = jobSpec.scheduler

  submission.jobs = [job]
  return yaml.dump(submission, {
    lineWidth: 10000,
  })
}

export const SidebarTabJobYaml = ({ job, jobSpecService }: SidebarTabJobRuns) => {
  const mounted = useRef(false)
  const openSnackbar = useCustomSnackbar()
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
        if (!mounted.current) {
          return
        }
        openSnackbar("Failed to retrieve Job spec for Job with ID: " + job.jobId + ": " + errMsg, "error")
        setJobSpecState({
          ...jobSpecState,
          loadState: "Idle",
        })
      }
    }
    loadJobSpec()
  }, [job])

  const submission = useMemo(() => {
    if (jobSpecState.jobSpec !== undefined) {
      return toJobSubmissionYaml(jobSpecState.jobSpec)
    }
    return undefined
  }, [jobSpecState])

  const downloadYamlFile = useCallback(() => {
    if (submission === undefined) {
      return
    }
    const element = document.createElement("a")
    const file = new Blob([submission], {
      type: "text/plain",
    })
    element.href = URL.createObjectURL(file)
    element.download = `${job.jobId}.yaml`
    document.body.appendChild(element)
    element.click()
  }, [submission, jobSpecState, job])

  const copyYaml = useCallback(async () => {
    if (submission === undefined) {
      return
    }
    await navigator.clipboard.writeText(submission)
    openSnackbar("Copied job submission YAML to clipboard!", "info", {
      autoHideDuration: 3000,
      preventDuplicate: true,
    })
  }, [submission, jobSpecState, job])

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {jobSpecState.loadState === "Loading" && <div></div>}
      {jobSpecState.jobSpec && (
        <div className={styles.jobYaml}>
          <div className={styles.jobYamlActions}>
            <div>
              <IconButton size="small" title="Copy to clipboard" onClick={copyYaml}>
                <ContentCopy />
              </IconButton>
              <IconButton size="small" title="Download as YAML file" onClick={downloadYamlFile}>
                <Download />
              </IconButton>
            </div>
          </div>
          {toJobSubmissionYaml(jobSpecState.jobSpec)}
        </div>
      )}
    </div>
  )
}
