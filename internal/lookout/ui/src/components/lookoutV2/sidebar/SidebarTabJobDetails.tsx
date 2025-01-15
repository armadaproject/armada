import { Alert } from "@mui/material"

import { ContainerDetails } from "./ContainerDetails"
import { KeyValuePairTable } from "./KeyValuePairTable"
import { SidebarTabHeading } from "./sidebarTabContentComponents"
import { Job } from "../../../models/lookoutV2Models"
import { formatBytes, formatCpu } from "../../../utils/resourceUtils"

export interface SidebarTabJobDetailsProps {
  job: Job
}

export const SidebarTabJobDetails = ({ job }: SidebarTabJobDetailsProps) => {
  const details = [
    { key: "Queue", value: job.queue, allowCopy: true },
    { key: "Job Set", value: job.jobSet, allowCopy: true },
    { key: "Owner", value: job.owner, allowCopy: true },
    ...(job.namespace ? [{ key: "Namespace", value: job.namespace, allowCopy: true }] : []),
    { key: "Priority", value: job.priority.toString() },
    { key: "Run Count", value: job.runs.length.toString() },
    ...(job.cancelReason ? [{ key: "Cancel Reason", value: job.cancelReason, allowCopy: true }] : []),
  ]
  return (
    <>
      <SidebarTabHeading>Info</SidebarTabHeading>
      <KeyValuePairTable data={details} />
      <SidebarTabHeading>Requests</SidebarTabHeading>
      <KeyValuePairTable
        data={[
          { key: "CPUs", value: formatCpu(job.cpu) },
          { key: "Memory", value: formatBytes(job.memory) },
          { key: "GPUs", value: job.gpu.toString() },
          { key: "Ephemeral storage", value: formatBytes(job.ephemeralStorage) },
        ]}
      />
      <SidebarTabHeading>Annotations</SidebarTabHeading>
      {Object.keys(job.annotations).length > 0 ? (
        <KeyValuePairTable
          data={Object.keys(job.annotations).map((annotationKey) => ({
            key: annotationKey,
            value: job.annotations[annotationKey],
            isAnnotation: true,
            allowCopy: true,
          }))}
        />
      ) : (
        <Alert severity="info">No annotations</Alert>
      )}
      <SidebarTabHeading>Containers</SidebarTabHeading>
      <ContainerDetails job={job} />
    </>
  )
}
