import { useMemo } from "react"

import { Typography } from "@mui/material"
import { Job } from "models/lookoutV2Models"

import { useCustomSnackbar } from "../../../hooks/useCustomSnackbar"
import { useJobSpec } from "../../../hooks/useJobSpec"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { formatBytes, formatCpu } from "../../../utils/resourceUtils"
import { KeyValuePairTable } from "./KeyValuePairTable"

export interface SidebarTabJobDetailsProps {
  job: Job
  jobSpecService: IGetJobSpecService
}

export interface ContainerDetails {
  name: string
  command: string
  args: string
  cpu: string
  memory: string
  ephemeralStorage: string
  gpu: string
}

export const SidebarTabJobDetails = ({ job, jobSpecService }: SidebarTabJobDetailsProps) => {
  const openSnackbar = useCustomSnackbar()
  const jobSpecState = useJobSpec(job, jobSpecService, openSnackbar)

  const containers: ContainerDetails[] = useMemo(() => {
    if (jobSpecState.loadState === "Loading" || jobSpecState.jobSpec === undefined) {
      return []
    }
    const containerDetails: ContainerDetails[] = []
    if (jobSpecState.jobSpec.podSpec && jobSpecState.jobSpec.podSpec.containers) {
      for (const container of jobSpecState.jobSpec.podSpec.containers as Record<string, unknown>[]) {
        const details: ContainerDetails = {
          name: "",
          command: "",
          args: "",
          cpu: "",
          memory: "",
          ephemeralStorage: "",
          gpu: "",
        }
        if (container.name) {
          details.name = container.name as string
        }
        if (container.command) {
          details.command = (container.command as string[]).join(" ")
        }
        if (container.args) {
          details.args = (container.args as string[]).join(" ")
        }
        if (container.resources && (container.resources as Record<string, unknown>).limits) {
          const limits = (container.resources as Record<string, unknown>).limits as Record<string, unknown>
          details.cpu = (limits.cpu as string) ?? ""
          details.memory = (limits.memory as string) ?? ""
          details.ephemeralStorage = (limits["ephemeral-storage"] as string) ?? ""
          details.gpu = (limits["nvidia.com/gpu"] as string) ?? ""
        }
        containerDetails.push(details)
      }
    }
    console.log(containerDetails)
    return containerDetails
  }, [jobSpecState])

  return (
    <>
      <Typography variant="subtitle2">Info:</Typography>
      <KeyValuePairTable
        data={[
          { key: "Queue", value: job.queue },
          { key: "Job Set", value: job.jobSet },
          { key: "Owner", value: job.owner },
          { key: "Priority", value: job.priority.toString() },
          { key: "Run Count", value: job.runs.length.toString() },
        ]}
      />

      <Typography variant="subtitle2">Requests:</Typography>
      <KeyValuePairTable
        data={[
          { key: "CPUs", value: formatCpu(job.cpu) },
          { key: "Memory", value: formatBytes(job.memory) },
          { key: "GPUs", value: job.gpu.toString() },
          { key: "Ephemeral storage", value: formatBytes(job.ephemeralStorage) },
        ]}
      />

      <Typography variant="subtitle2">Annotations:</Typography>
      {Object.keys(job.annotations).length > 0 ? (
        <KeyValuePairTable
          data={Object.keys(job.annotations).map((annotationKey) => ({
            key: annotationKey,
            value: job.annotations[annotationKey],
          }))}
        />
      ) : (
        " No annotations"
      )}
    </>
  )
}
