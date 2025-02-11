import { Chip, styled } from "@mui/material"

import { JobState, jobStateColors } from "../../models/lookoutV2Models"

const StyledChip = styled(Chip)({ padding: "0 1ch" })

export interface JobStateCountChipProps {
  state: JobState
  count: number
  onClick?: () => void
}
export const JobStateCountChip = ({ state, count, onClick }: JobStateCountChipProps) => {
  const label = count.toString()

  return count > 0 ? (
    <StyledChip label={label} color={jobStateColors[state]} clickable onClick={onClick} variant="shaded" size="small" />
  ) : (
    <>{label}</>
  )
}
