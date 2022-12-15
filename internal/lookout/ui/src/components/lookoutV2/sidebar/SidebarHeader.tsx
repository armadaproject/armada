import { Close } from "@mui/icons-material"
import { Box, IconButton, Typography } from "@mui/material"
import { Job } from "models/lookoutV2Models"
import { formatJobState, formatTimeSince } from "utils/jobsTableFormatters"

export interface SidebarHeaderProps {
  job: Job
  onClose: () => void
}
export const SidebarHeader = ({job, onClose}: SidebarHeaderProps) => {
  return (
    <Box sx={{display: "flex", flexDirection: "row", gap: "1em"}}>
      <div>
      <Typography sx={{ fontSize: 14 }} color="text.secondary">
          Job ID
        </Typography>
        <Box sx={{wordBreak: "break-all"}}>{job.jobId}</Box>
      </div>
      <div>{formatJobState(job.state)} for {formatTimeSince(job.lastTransitionTime)}</div>
      <IconButton
        onClick={onClose}
      >
        <Close />
      </IconButton>

    </Box>
  )
}