import { Typography } from "@mui/material"
import { Job } from "models/lookoutV2Models"

import { ContainerDetails } from "./ContainerDetails"
import { KeyValuePairTable } from "./KeyValuePairTable"
import { IGetJobSpecService } from "../../../services/lookoutV2/GetJobSpecService"
import { formatBytes, formatCpu } from "../../../utils/resourceUtils"

export interface SidebarTabJobDetailsProps {
  job: Job
  jobSpecService: IGetJobSpecService
}

export const SidebarTabJobDetails = ({ job, jobSpecService }: SidebarTabJobDetailsProps) => {
  const details = [
    { key: "Queue", value: job.queue },
    { key: "Job Set", value: job.jobSet },
    { key: "Owner", value: job.owner },
    ...(job.namespace ? [{ key: "Namespace", value: job.namespace }] : []),
    { key: "Priority", value: job.priority.toString() },
    { key: "Run Count", value: job.runs.length.toString() },
    ...(job.cancelReason ? [{ key: "Cancel Reason", value: job.cancelReason }] : []),
  ]
  return (
    <>
      <Typography variant="subtitle2">Info:</Typography>
      <KeyValuePairTable data={details} />
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
            isAnnotation: true,
          }))}
        />
      ) : (
        " No annotations"
      )}
      <ContainerDetails job={job} jobSpecService={jobSpecService} />
    </>
  )
}
