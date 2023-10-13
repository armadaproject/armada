import React from "react"

import { Grid } from "@visx/grid"
import { scaleBand, scaleLinear } from "@visx/scale"
import { Circle } from "@visx/shape"
import { BoxPlot } from "@visx/stats"
import { TooltipWithBounds, withTooltip } from "@visx/tooltip"
import { WithTooltipProvidedProps } from "@visx/tooltip/lib/enhancers/withTooltip"
import { defaultStyles as defaultTooltipStyles } from "@visx/tooltip/lib/tooltips/Tooltip"

import DurationTooltip, { DurationTooltipProps } from "./DurationTooltip"
import { DurationStats } from "../../services/JobService"
import { Padding } from "../../utils"

type DurationBoxPlotData = {
  width: number
  height: number
  duration: DurationStats
  primaryColor: string
  secondaryColor: string
  padding: Padding
  boxWidth: number
  maxValue: number
  minValue: number
  nTicks: number
}

type DurationBoxPlotProps = DurationBoxPlotData & WithTooltipProvidedProps<DurationTooltipProps>

function DurationBoxPlot(props: DurationBoxPlotProps) {
  const scale = scaleLinear<number>({
    range: [props.padding.left, props.width - props.padding.right],
    round: true,
    domain: [props.minValue, props.maxValue],
  })

  const nameScale = scaleBand<number>({
    range: [props.height / 2, props.height / 2],
    domain: [0],
  })

  return (
    <div>
      <svg width={props.width} height={props.height}>
        <g>
          <Grid
            xScale={scale}
            yScale={nameScale}
            width={props.width}
            height={props.height}
            numTicksColumns={10}
            numTicksRows={1}
            stroke={"#E0E0E0"}
          />
          <BoxPlot
            horizontal={true}
            min={props.duration.shortest}
            max={props.duration.longest}
            top={props.height * 0.5 - props.boxWidth * 0.5}
            firstQuartile={props.duration.q1}
            thirdQuartile={props.duration.q3}
            median={props.duration.median}
            boxWidth={props.boxWidth}
            fill={props.primaryColor}
            fillOpacity={0.3}
            stroke={props.primaryColor}
            strokeWidth={2}
            valueScale={scale}
            minProps={{
              onMouseOver: () => {
                props.showTooltip({
                  tooltipTop: props.padding.top,
                  tooltipLeft: scale(props.duration.shortest),
                  tooltipData: {
                    min: props.duration.shortest,
                  },
                })
              },
              onMouseLeave: () => {
                props.hideTooltip()
              },
            }}
            maxProps={{
              onMouseOver: () => {
                props.showTooltip({
                  tooltipTop: props.padding.top,
                  tooltipLeft: scale(props.duration.longest),
                  tooltipData: {
                    max: props.duration.longest,
                  },
                })
              },
              onMouseLeave: () => {
                props.hideTooltip()
              },
            }}
            boxProps={{
              onMouseOver: () => {
                props.showTooltip({
                  tooltipTop: 0,
                  tooltipLeft: scale(props.duration.median),
                  tooltipData: {
                    min: props.duration.shortest,
                    max: props.duration.longest,
                    median: props.duration.median,
                    firstQuartile: props.duration.q1,
                    thirdQuartile: props.duration.q3,
                    average: props.duration.average,
                  },
                })
              },
              onMouseLeave: () => {
                props.hideTooltip()
              },
            }}
            medianProps={{
              onMouseOver: () => {
                props.showTooltip({
                  tooltipTop: props.padding.top,
                  tooltipLeft: scale(props.duration.median),
                  tooltipData: {
                    median: props.duration.median,
                  },
                })
              },
              onMouseLeave: () => {
                props.hideTooltip()
              },
            }}
          />
          <Circle
            cx={scale(props.duration.average)}
            cy={props.height * 0.5}
            r={5}
            fill={props.secondaryColor}
            onMouseOver={() => {
              props.showTooltip({
                tooltipTop: props.padding.top,
                tooltipLeft: scale(props.duration.average),
                tooltipData: {
                  average: props.duration.average,
                },
              })
            }}
            onMouseLeave={() => {
              props.hideTooltip()
            }}
          />
        </g>
      </svg>

      {props.tooltipOpen && props.tooltipData && (
        <TooltipWithBounds
          top={props.tooltipTop}
          left={props.tooltipLeft}
          style={{ ...defaultTooltipStyles, backgroundColor: "#283238", color: "white" }}
        >
          <DurationTooltip {...props.tooltipData} />
        </TooltipWithBounds>
      )}
    </div>
  )
}

export default withTooltip<DurationBoxPlotData, DurationTooltipProps>(DurationBoxPlot)
