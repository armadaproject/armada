import { Alert } from "@mui/material"

import { JobState } from "../../../models/lookoutV2Models"

const jobStatusMayRunInFuture: Record<JobState, boolean> = {
  [JobState.Queued]: true,
  [JobState.Leased]: true,
  [JobState.Pending]: true,
  [JobState.Running]: true,
  [JobState.Succeeded]: false,
  [JobState.Failed]: false,
  [JobState.Cancelled]: false,
  [JobState.Preempted]: false,
  [JobState.Rejected]: false,
}

export interface NoRunsAlertProps {
  jobState: JobState
}

export const NoRunsAlert = ({ jobState }: NoRunsAlertProps) => (
  <Alert severity="info">{jobStatusMayRunInFuture[jobState] ? "This job has not run." : "This job did not run."}</Alert>
)
