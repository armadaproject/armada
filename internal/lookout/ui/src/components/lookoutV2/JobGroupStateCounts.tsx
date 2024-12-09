import { getContrastText } from "utils/styleUtils"

import styles from "./JobGroupStateCounts.module.css"
import { JobState, jobStateColors } from "../../models/lookoutV2Models"

interface JobGroupStateCountsProps {
  stateCounts: Record<string, number>
}

export const JobGroupStateCounts = ({ stateCounts }: JobGroupStateCountsProps) => {
  const content = []
  for (const [_, state] of Object.entries(JobState)) {
    if (!((state as string) in stateCounts)) {
      continue
    }
    const backgroundColor = jobStateColors[state]
    const val = (
      <span
        className={styles.state}
        key={state}
        style={{
          backgroundColor,
          color: getContrastText(backgroundColor),
        }}
      >
        {stateCounts[state as string]}
      </span>
    )
    content.push(val)
  }
  return <div className={styles.container}>{content}</div>
}
