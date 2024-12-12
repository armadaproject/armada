import { Chip } from "@mui/material"

import { JobState, jobStateColors } from "../../models/lookoutV2Models"

export interface JobStateCountChipProps {
  state: JobState
  count: number
  onClick?: () => void
}
export const JobStateCountChip = ({ state, count, onClick }: JobStateCountChipProps) => {
  const label = count.toString()

  return count > 0 ? (
    <Chip label={label} color={jobStateColors[state]} clickable component="button" onClick={onClick} variant="filled" />
  ) : (
    <>{label}</>
  )
}
