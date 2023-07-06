import React from "react"

import { Table, TableHeaderRowProps, TableRowProps } from "react-virtualized"

import DurationAxis from "./DurationAxis"
import DurationBoxPlot from "./DurationBoxPlot"
import { DurationStats } from "../../services/JobService"

import "./DurationPlotsTable.css"

interface DurationPlotsTableProps {
  height: number
  width: number
  names: string[]
  durations: DurationStats[]
  primaryColor: string
  secondaryColor: string
  percentagePlotWidth: number
}

const PADDING = {
  top: 20,
  bottom: 20,
  left: 30,
  right: 35,
}

const N_TICKS = 10

function renderHeader(
  minValue: number,
  maxValue: number,
  percentagePlotWidth: number,
  headerRowProps: TableHeaderRowProps,
) {
  const width = headerRowProps.style.width as number
  const height = headerRowProps.style.height as number
  const plotWidth = Math.round(width * percentagePlotWidth)
  const nameWidth = width - plotWidth

  return (
    <div className="duration-plots-table-row" style={headerRowProps.style}>
      <div
        style={{
          width: nameWidth,
          height: height,
        }}
      />
      <div
        style={{
          width: plotWidth,
          height: height,
        }}
      >
        <DurationAxis
          height={height}
          width={plotWidth}
          padding={PADDING}
          minValue={minValue}
          maxValue={maxValue}
          nTicks={N_TICKS}
        />
      </div>
    </div>
  )
}

function renderRow(minValue: number, maxValue: number, props: DurationPlotsTableProps, rowProps: TableRowProps) {
  const width = rowProps.style.width as number
  const height = rowProps.style.height as number
  const plotWidth = Math.round(width * props.percentagePlotWidth)
  const nameWidth = width - plotWidth

  return (
    <div key={rowProps.key} className="duration-plots-table-row" style={rowProps.style}>
      <div
        className="duration-plots-table-row-name"
        style={{
          width: nameWidth,
        }}
      >
        {props.names[rowProps.index]}
      </div>
      <div
        style={{
          height: height,
          width: plotWidth,
        }}
      >
        <DurationBoxPlot
          width={plotWidth}
          height={height}
          duration={props.durations[rowProps.index]}
          primaryColor={props.primaryColor}
          secondaryColor={props.secondaryColor}
          padding={PADDING}
          boxWidth={32}
          maxValue={maxValue}
          minValue={minValue}
          nTicks={N_TICKS}
        />
      </div>
    </div>
  )
}

export default function DurationPlotsTable(props: DurationPlotsTableProps) {
  if (props.durations.length === 0) {
    return <div />
  }

  let minDuration = Math.min(...props.durations.map((d) => d.shortest))
  let maxDuration = Math.max(...props.durations.map((d) => d.longest))
  if (isNaN(minDuration) || isNaN(maxDuration)) {
    minDuration = 0
    maxDuration = 0
  }
  const fivePercent = Math.max(1, Math.round((maxDuration - minDuration) * 0.05))
  const minValue = minDuration - fivePercent
  const maxValue = maxDuration + fivePercent

  return (
    <div
      style={{
        height: props.height,
        width: props.width,
      }}
    >
      <Table
        height={props.height}
        width={props.width}
        rowCount={props.durations.length}
        rowGetter={({ index }) => props.names[index]}
        rowHeight={80}
        headerHeight={50}
        className="duration-plots-table"
        headerRowRenderer={(headerRowProps) =>
          renderHeader(minValue, maxValue, props.percentagePlotWidth, headerRowProps)
        }
        rowRenderer={(rowProps) => renderRow(minValue, maxValue, props, rowProps)}
      />
    </div>
  )
}
