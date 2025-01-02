import { useEffect } from "react"

import { CircularProgress } from "@mui/material"
import yaml from "js-yaml"

import styles from "./SidebarTabJobYaml.module.css"
import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { Job } from "../../../models/lookoutV2Models"
import { useGetJobSpec } from "../../../services/lookoutV2/useGetJobSpec"
import { CodeBlock } from "../../CodeBlock"

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

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {getJobSpecResult.status === "pending" && (
        <div className={styles.loading}>
          <CircularProgress size={24} />
        </div>
      )}
      {getJobSpecResult.status === "success" && (
        <CodeBlock
          language="yaml"
          code={toJobSubmissionYaml(getJobSpecResult.data)}
          showLineNumbers
          downloadable={getJobSpecResult.status === "success"}
          downloadBlobType="text/plain"
          downloadFileName={`${job.jobId}.yaml`}
        />
      )}
    </div>
  )
}
