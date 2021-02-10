import React from "react"
import { BoxPlot } from '@visx/stats';
import { scaleBand, scaleLinear } from '@visx/scale';

import { DurationStats } from "../../services/JobService"

interface JobDurationsProps {
  jobSets: string[]
  durationStats: DurationStats[]
}

export default function JobDurations(props: JobDurationsProps) {
  const minDuration = Math.min(...props.durationStats.map(d => d.shortest))
  const maxDuration = Math.max(...props.durationStats.map(d => d.longest))

  const xScale = scaleBand<string>({
    range: [0, 400],
    round: true,
    domain: props.jobSets,
    padding: 0.4,
  });

  const yScale = scaleLinear<number>({
    range: [600, 0],
    round: true,
    domain: [0, maxDuration],
  })
  const someDuration = props.durationStats[0]

  console.log(someDuration.shortest, yScale(someDuration.shortest))
  console.log(someDuration.q1, yScale(someDuration.q1))
  console.log(someDuration.median, yScale(someDuration.median))

  console.log(maxDuration)
  console.log(someDuration)

  return (
    <svg width={"600px"} height={"600px"}>
      <BoxPlot
        valueScale={yScale}
        min={someDuration.shortest}
        max={someDuration.longest}
        firstQuartile={someDuration.q1}
        thirdQuartile={someDuration.q3}
        median={someDuration.median}
        boxWidth={40}
        fill="#333333"
        fillOpacity={0.3}
        stroke="#000000"
        strokeWidth={2} />
    </svg>
  )
}
