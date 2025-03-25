import { Alert } from "@mui/material"

import { ContainerDetails } from "./ContainerDetails"
import { KeyValuePairTable } from "./KeyValuePairTable"
import { SidebarTabHeading } from "./sidebarTabContentComponents"
import { useFormatNumberWithUserSettings } from "../../../hooks/formatNumberWithUserSettings"
import { Job } from "../../../models/lookoutModels"
import { formatBytes, formatCpu } from "../../../utils/resourceUtils"

export interface SidebarTabJobDetailsProps {
  job: Job
}

export const SidebarTabJobDetails = ({ job }: SidebarTabJobDetailsProps) => {
  const formatNumber = useFormatNumberWithUserSettings()

  const details = [
    { key: "Queue", value: job.queue, allowCopy: true },
    { key: "Job Set", value: job.jobSet, allowCopy: true },
    { key: "Owner", value: job.owner, allowCopy: true },
    ...(job.namespace ? [{ key: "Namespace", value: job.namespace, allowCopy: true }] : []),
    { key: "Priority", value: job.priority.toString() }, // this value is deliberately left unformatted to so it is displayed precisely
    { key: "Run Count", value: formatNumber(job.runs.length) },
    ...(job.cancelReason ? [{ key: "Cancel Reason", value: job.cancelReason, allowCopy: true }] : []),
    ...(job.cancelUser ? [{ key: "Cancelled By", value: job.cancelUser, allowCopy: true }] : []),
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
