import { useCallback, useEffect, useMemo } from "react"

import { ContentCopy, Download } from "@mui/icons-material"
import { CircularProgress } from "@mui/material"
import { IconButton } from "@mui/material"
import yaml from "js-yaml"
import { Job } from "models/lookoutV2Models"

import styles from "./SidebarTabJobYaml.module.css"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { useGetJobSpec } from "../../../services/lookoutV2/useGetJobSpec"

export interface SidebarTabJobYamlProps {
  job: Job
}

function toJobSubmissionYaml(jobSpec: Record<string, any>): string {
  const submission: Record<string, any> = {}
  submission.queue = jobSpec.queue
  submission.jobSetId = jobSpec.jobSetId

  const job: Record<string, any> = {}
  job.priority = jobSpec.priority
  job.namespace = jobSpec.namespace
  job.annotations = jobSpec.annotations
  job.labels = jobSpec.labels
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
    lineWidth: 100000,
  })
}

export const SidebarTabJobYaml = ({ job }: SidebarTabJobYamlProps) => {
  const openSnackbar = useCustomSnackbar()

  const getJobSpecResult = useGetJobSpec(job.jobId, Boolean(job.jobId))
  useEffect(() => {
    if (getJobSpecResult.status === "error") {
      openSnackbar(`Failed to retrieve Job spec for Job with ID: ${job.jobId}: ${getJobSpecResult.error}`, "error")
    }
  }, [getJobSpecResult.status, getJobSpecResult.error])

  const submission = useMemo(() => {
    if (getJobSpecResult.status === "success") {
      return toJobSubmissionYaml(getJobSpecResult.data)
    }
    return undefined
  }, [getJobSpecResult.status, getJobSpecResult.data])

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
  }, [submission, job])

  const copyYaml = useCallback(async () => {
    if (submission === undefined) {
      return
    }
    await navigator.clipboard.writeText(submission)
    openSnackbar("Copied job submission YAML to clipboard!", "info", {
      autoHideDuration: 3000,
      preventDuplicate: true,
    })
  }, [submission, job])

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {getJobSpecResult.status === "pending" && (
        <div className={styles.loading}>
          <CircularProgress size={24} />
        </div>
      )}
      {getJobSpecResult.status === "success" && (
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
          {toJobSubmissionYaml(getJobSpecResult.data)}
        </div>
      )}
    </div>
  )
}
