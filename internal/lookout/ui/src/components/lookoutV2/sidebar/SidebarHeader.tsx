import { memo, ReactNode } from "react"

import { Close } from "@mui/icons-material"
import { Box, IconButton, Typography } from "@mui/material"
import { Job } from "models/lookoutV2Models"
import { formatJobState, formatTimeSince } from "utils/jobsTableFormatters"

import { JobStateLabel } from "../JobStateLabel"

export interface SidebarHeaderProps {
  job: Job
  onClose: () => void
  className?: string
}

export const SidebarHeader = memo(({ job, onClose, className }: SidebarHeaderProps) => {
  return (
    <Box className={className}>
      <HeaderSection title={"Job ID"} value={<Box sx={{ wordBreak: "break-all" }}>{job.jobId}</Box>} />
      <HeaderSection
        title={"State"}
        value={
          <JobStateLabel state={job.state}>
            {formatJobState(job.state)} for {formatTimeSince(job.lastTransitionTime)}
          </JobStateLabel>
        }
      />
      <IconButton sx={{ marginLeft: "auto" }} onClick={onClose}>
        <Close />
      </IconButton>
    </Box>
  )
})

interface HeaderSectionProps {
  title: string
  value: ReactNode
}
const HeaderSection = ({ title, value }: HeaderSectionProps) => {
  return (
    <div>
      <Typography sx={{ fontSize: 14 }} color="text.secondary">
        {title}
      </Typography>
      <Typography variant="subtitle2">{value}</Typography>
    </div>
  )
}
