import { Typography } from "@mui/material"
import { Job } from "models/lookoutV2Models"

import { formatBytes, formatCpu } from "../../../utils/resourceUtils"
import { KeyValuePairTable } from "./KeyValuePairTable"

export interface SidebarTabJobDetailsProps {
  job: Job
}

export const SidebarTabJobDetails = ({ job }: SidebarTabJobDetailsProps) => {
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
            isAnnotation: true,
          }))}
        />
      ) : (
        " No annotations"
      )}
    </>
  )
}
