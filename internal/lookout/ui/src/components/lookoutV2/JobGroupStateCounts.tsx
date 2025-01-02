import { Chip, Tooltip, Typography } from "@mui/material"

import styles from "./JobGroupStateCounts.module.css"
import { JobState, jobStateColors, jobStateIcons } from "../../models/lookoutV2Models"
import { formatJobState } from "../../utils/jobsTableFormatters"

interface JobGroupStateCountsProps {
  stateCounts: Partial<Record<JobState, number>>
}

export const JobGroupStateCounts = ({ stateCounts }: JobGroupStateCountsProps) => (
  <div className={styles.container}>
    {Object.values(JobState)
      .flatMap(
        (jobState) => (jobState in stateCounts ? [[jobState, stateCounts[jobState]]] : []) as [JobState, number][],
      )
      .map(([_jobState, count]) => {
        const jobState = _jobState as JobState
        const Icon = jobStateIcons[jobState]
        return (
          <Tooltip key={jobState} title={formatJobState(jobState)}>
            <Chip
              size="small"
              variant="outlined"
              label={
                <Typography fontSize="inherit" color="text.primary" fontWeight="medium">
                  {count.toString()}
                </Typography>
              }
              color={jobStateColors[jobState]}
              icon={<Icon />}
            />
          </Tooltip>
        )
      })}
  </div>
)
