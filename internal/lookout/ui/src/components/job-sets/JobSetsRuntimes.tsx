import React from 'react'

import { DurationStats, JobSet } from "../../services/JobService"

import DurationBoxPlot from "./DurationBoxPlot";

interface JobSetsRuntimesProps {
  jobSets: JobSet[]
}


export default function JobSetsRuntimes(props: JobSetsRuntimesProps) {
  const filtered = props.jobSets.filter(js => js.runningStats)

  return (
    <div>
      <DurationBoxPlot
        primaryColor={"#00bcd4"}
        secondaryColor={"#673ab7"}
        totalWidth={1200}
        singlePlotHeight={64}
        names={filtered.map(js => js.jobSet)}
        durations={filtered.map(js => js.runningStats as DurationStats)} />
    </div>
  )
}
