import { Chip } from "@mui/material"

import { formatJobState } from "../common/jobsTableFormatters"
import { JobState, jobStateColors, jobStateIcons } from "../models/lookoutModels"

export interface JobStateChipProps {
  state: JobState
  hideLabel?: boolean
}
export const JobStateChip = ({ state, hideLabel = false }: JobStateChipProps) => {
  if (!state) {
    return null
  }
  const Icon = jobStateIcons[state]
  return (
    <Chip
      label={hideLabel ? undefined : formatJobState(state)}
      size="small"
      color={jobStateColors[state]}
      icon={<Icon />}
      variant="shaded"
    />
  )
}
