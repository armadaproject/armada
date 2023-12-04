import styles from "./JobGroupStateCounts.module.css"
import { JobState } from "../../models/lookoutV2Models"
import { colorForJobState } from "../../utils/jobsTableFormatters"

interface JobGroupStateCountsProps {
  stateCounts: Record<string, number>
}

export const JobGroupStateCounts = ({ stateCounts }: JobGroupStateCountsProps) => {
  const content = []
  for (const [_, state] of Object.entries(JobState)) {
    if (!((state as string) in stateCounts)) {
      continue
    }
    const color = colorForJobState(state)
    const val = (
      <span
        className={styles.state}
        key={state}
        style={{
          backgroundColor: color,
        }}
      >
        {stateCounts[state as string]}
      </span>
    )
    content.push(val)
  }
  return <div className={styles.container}>{content}</div>
}
