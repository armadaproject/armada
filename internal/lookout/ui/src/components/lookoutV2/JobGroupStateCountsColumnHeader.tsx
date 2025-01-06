import { styled, Tooltip } from "@mui/material"

import { JobState, jobStateColors, jobStateIcons } from "../../models/lookoutV2Models"
import { formatJobState } from "../../utils/jobsTableFormatters"

const OuterContainer = styled("div")({
  display: "grid",
  gridAutoColumns: "minmax(0, 1fr)",
  gridAutoFlow: "column",
  textAlign: "center",
})

export interface JobGroupStateCountsColumnHeaderProps {
  jobStatesToDisplay?: JobState[]
}

export const JobGroupStateCountsColumnHeader = ({
  jobStatesToDisplay = Object.values(JobState),
}: JobGroupStateCountsColumnHeaderProps) => (
  <OuterContainer>
    {Object.values(JobState).map((_jobState) => {
      const jobState = _jobState as JobState
      const Icon = jobStateIcons[jobState]
      return (
        jobStatesToDisplay.includes(jobState) && (
          <Tooltip key={jobState} title={formatJobState(jobState)} placement="top">
            <div>
              <Icon fontSize="inherit" color={jobStateColors[jobState]} />
            </div>
          </Tooltip>
        )
      )
    })}
  </OuterContainer>
)
