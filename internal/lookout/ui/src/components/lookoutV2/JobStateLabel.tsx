import { ReactNode } from "react"

import { Box } from "@mui/material"
import { defaultJobStateColor, JobState, jobStateColors } from "models/lookoutV2Models"
import { getContrastText } from "utils/styleUtils"

export interface JobStateLabelProps {
  state?: JobState
  children: ReactNode
}
export const JobStateLabel = ({ state, children }: JobStateLabelProps) => {
  const backgroundColor = state ? jobStateColors[state] : defaultJobStateColor
  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        backgroundColor,
        padding: backgroundColor ? "0 0.5em" : "",
        color: getContrastText(backgroundColor),
      }}
    >
      {children}
    </Box>
  )
}
