import React from "react"

import { secondsToDurationString } from "../../utils"

import "./DurationTooltip.css"

export interface DurationTooltipProps {
  min?: number
  median?: number
  average?: number
  max?: number
  firstQuartile?: number
  thirdQuartile?: number
}

export default function DurationTooltip(props: DurationTooltipProps) {
  let content = (
    <>
      {props.max && <div>max: {secondsToDurationString(props.max)}</div>}
      {props.thirdQuartile && <div>third quartile: {secondsToDurationString(props.thirdQuartile)}</div>}
      {props.median && <div>median: {secondsToDurationString(props.median)}</div>}
      {props.average && <div>average: {secondsToDurationString(props.average)}</div>}
      {props.firstQuartile && <div>first quartile: {secondsToDurationString(props.firstQuartile)}</div>}
      {props.min && <div>min: {secondsToDurationString(props.min)}</div>}
    </>
  )

  if (allDefined(props)) {
    content = (
      <table>
        <tbody>
          <tr>
            <td className="cell-left">{props.min && <div>min: {secondsToDurationString(props.min)}</div>}</td>
            <td className="cell-right">{props.max && <div>max: {secondsToDurationString(props.max)}</div>}</td>
          </tr>
          <tr>
            <td className="cell-left">
              {props.firstQuartile && <div>first quartile: {secondsToDurationString(props.firstQuartile)}</div>}
            </td>
            <td className="cell-right">
              {props.thirdQuartile && <div>third quartile: {secondsToDurationString(props.thirdQuartile)}</div>}
            </td>
          </tr>
          <tr>
            <td className="cell-left">{props.median && <div>median: {secondsToDurationString(props.median)}</div>}</td>
            <td className="cell-right">
              {props.average && <div>average: {secondsToDurationString(props.average)}</div>}
            </td>
          </tr>
        </tbody>
      </table>
    )
  }
  return <div className="duration-tooltip">{content}</div>
}

function allDefined({ min, median, average, max, firstQuartile, thirdQuartile }: DurationTooltipProps): boolean {
  return !!(min && median && average && max && firstQuartile && thirdQuartile)
}
