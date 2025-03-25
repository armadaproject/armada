import { Chip, styled, Tooltip } from "@mui/material"

import { useFormatNumberWithUserSettings } from "../../hooks/formatNumberWithUserSettings"
import { JobState, jobStateColors } from "../../models/lookoutModels"
import { formatJobState } from "../../utils/jobsTableFormatters"

const CountsContainer = styled("div")({
  display: "grid",
  gridAutoColumns: "minmax(0, 1fr)",
  gridAutoFlow: "column",
  textAlign: "center",
})

const StateCountChip = styled(Chip)({
  width: "100%",
  borderRadius: 0,

  "& .MuiChip-label": {
    overflow: "visible",
  },
})

interface JobGroupStateCountsProps {
  jobStatesToDisplay?: JobState[]
  stateCounts: Partial<Record<JobState, number>>
}

export const JobGroupStateCounts = ({
  stateCounts,
  jobStatesToDisplay = Object.values(JobState),
}: JobGroupStateCountsProps) => {
  const formatNumber = useFormatNumberWithUserSettings()
  return (
    <CountsContainer>
      {Object.values(JobState).map((_jobState) => {
        const jobState = _jobState as JobState
        const count = stateCounts[jobState] ?? 0
        return (
          jobStatesToDisplay.includes(jobState) && (
            <Tooltip key={jobState} title={`${formatJobState(jobState)} (${formatNumber(count)})`}>
              <div>
                <StateCountChip
                  size="small"
                  variant="shaded"
                  label={formatNumber(count)}
                  color={jobStateColors[jobState]}
                  disabled={count === 0}
                />
              </div>
            </Tooltip>
          )
        )
      })}
    </CountsContainer>
  )
}
