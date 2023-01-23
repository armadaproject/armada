import { ReactNode } from "react"

import { Box } from "@mui/material"
import { JobState } from "models/lookoutV2Models"
import { colorForJobState } from "utils/jobsTableFormatters"

export interface JobStateLabelProps {
  state?: JobState
  children: ReactNode
}
export const JobStateLabel = ({ state, children }: JobStateLabelProps) => {
  const color = colorForJobState(state)
  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        backgroundColor: color,
        padding: color ? "0 0.5em" : "",
      }}
    >
      {children}
    </Box>
  )
}
