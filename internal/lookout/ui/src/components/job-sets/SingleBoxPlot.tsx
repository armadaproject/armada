import React from 'react'
import { DurationStats } from "../../services/JobService";

interface SingleBoxPlot {
  width: number
  height: number
  duration: DurationStats
  primaryColor: string
  secondaryColor: string
  durationScale: import("d3-scale").ScaleLinear<number, number, never>;
}

export default function SingleBoxPlot() {

}
