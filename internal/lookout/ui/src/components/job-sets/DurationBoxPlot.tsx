import React from 'react';
import { Group } from '@visx/group';
import { BoxPlot } from '@visx/stats';
import { Circle } from '@visx/shape'
import { scaleBand, scaleLinear } from '@visx/scale';
import { defaultStyles as defaultTooltipStyles, TooltipWithBounds, withTooltip } from '@visx/tooltip';
import { WithTooltipProvidedProps } from '@visx/tooltip/lib/enhancers/withTooltip';
import { AxisLeft, AxisTop } from '@visx/axis';
import { GridColumns } from '@visx/grid';
import { DurationStats } from "../../services/JobService";
import { secondsToDurationString } from "../../utils";

interface TooltipData {
  name?: string;
  min?: number;
  median?: number;
  average?: number;
  max?: number;
  firstQuartile?: number;
  thirdQuartile?: number;
}

export type StatsPlotProps = {
  totalWidth: number;
  singlePlotHeight: number;
  names: string[],
  durations: DurationStats[],
  primaryColor: string
  secondaryColor: string
}

type TestBoxPlotProps = StatsPlotProps & WithTooltipProvidedProps<TooltipData>

const TOP_PADDING = 100
const LEFT_PADDING = 200
const BOTTOM_PADDING = 30
const RIGHT_PADDING = 30

function DurationBoxPlot(props: TestBoxPlotProps) {
  const totalHeight = props.singlePlotHeight * props.durations.length + TOP_PADDING + BOTTOM_PADDING

  const nameScale = scaleBand<string>({
    range: [TOP_PADDING, totalHeight - BOTTOM_PADDING],
    domain: props.names,
  });

  let minDuration = Math.min(...props.durations.map(d => d.shortest))
  let maxDuration = Math.max(...props.durations.map(d => d.longest))
  if (isNaN(minDuration) || isNaN(maxDuration)) {
    minDuration = 0
    maxDuration = 0
  }

  const fivePercent = Math.min(1, Math.round((maxDuration - minDuration) * 0.05))

  const durationScale = scaleLinear<number>({
    range: [LEFT_PADDING, props.totalWidth - RIGHT_PADDING],
    round: true,
    domain: [Math.max(0, minDuration - fivePercent), maxDuration + fivePercent],
  });

  const bandwidth = nameScale.bandwidth();
  const constrainedWidth = Math.min(40, bandwidth);
  const boxWidth = constrainedWidth * 0.6

  return props.totalWidth < 10 || props.durations.length === 0 ? null : (
    <div>
      <svg width={props.totalWidth} height={totalHeight}>
        <GridColumns
          scale={durationScale}
          height={totalHeight - BOTTOM_PADDING - TOP_PADDING + bandwidth * 0.5}
          stroke="#e0e0e0"
          top={Math.floor(TOP_PADDING / 2)}/>
        <AxisTop
          top={Math.floor(TOP_PADDING / 2)}
          scale={durationScale}
          numTicks={10}
          tickFormat={(v) => secondsToDurationString(v.valueOf())}
          tickLabelProps={() => ({
            dy: "-0.75em",
            textAnchor: "middle",
            fontSize: 12,
          })}/>
        <AxisLeft
          top={- bandwidth * 0.5 + boxWidth * 0.5}
          left={LEFT_PADDING - 30}
          scale={nameScale}
          numTicks={props.names.length}
          tickLabelProps={() => ({
            dx: '-0.25em',
            dy: '0.25em',
            fontSize: 10,
            textAnchor: 'end',
            width: LEFT_PADDING - 40,
          })}/>
        <Group>
          {props.durations.map((d, i) => {
            const name = props.names[i]
            return (
              <g key={i}>
                <BoxPlot
                  horizontal={true}
                  min={d.shortest}
                  max={d.longest}
                  top={nameScale(name)}
                  firstQuartile={d.q1}
                  thirdQuartile={d.q3}
                  median={d.median}
                  boxWidth={boxWidth}
                  fill={props.primaryColor}
                  fillOpacity={0.3}
                  stroke={props.primaryColor}
                  strokeWidth={2}
                  valueScale={durationScale}
                  minProps={{
                    onMouseOver: () => {
                      props.showTooltip({
                        tooltipTop: nameScale(name) ?? 40,
                        tooltipLeft: durationScale(d.shortest),
                        tooltipData: {
                          min: d.shortest,
                          name: name,
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
                        tooltipTop: nameScale(name) ?? 40,
                        tooltipLeft: durationScale(d.longest),
                        tooltipData: {
                          max: d.longest,
                          name: name,
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
                        tooltipTop: (nameScale(name) ?? 40) - bandwidth * 0.5,
                        tooltipLeft: durationScale(d.median),
                        tooltipData: {
                          min: d.shortest,
                          max: d.longest,
                          median: d.median,
                          firstQuartile: d.q1,
                          thirdQuartile: d.q3,
                          average: d.average,
                          name: name,
                        },
                      })
                    },
                    onMouseLeave: () => {
                      props.hideTooltip();
                    },
                  }}
                  medianProps={{
                    onMouseOver: () => {
                      props.showTooltip({
                        tooltipTop: nameScale(name) ?? 40,
                        tooltipLeft: durationScale(d.median),
                        tooltipData: {
                          median: d.median,
                          name: name,
                        },
                      })
                    },
                    onMouseLeave: () => {
                      props.hideTooltip();
                    },
                  }}
                />
                <Circle
                  cx={durationScale(d.average)}
                  cy={(nameScale(name) ?? 40) + boxWidth * 0.5}
                  r={5}
                  fill={props.secondaryColor}
                  onMouseOver={() => {
                    props.showTooltip({
                      tooltipTop: nameScale(name) ?? 40,
                      tooltipLeft: durationScale(d.average),
                      tooltipData: {
                        average: d.average,
                        name: name,
                      },
                    })
                  }}
                  onMouseLeave={() => {
                    props.hideTooltip()
                  }}/>
              </g>
            )
          })}
        </Group>
      </svg>

      {props.tooltipOpen && props.tooltipData && (
        <TooltipWithBounds
          top={props.tooltipTop}
          left={props.tooltipLeft}
          style={{ ...defaultTooltipStyles, backgroundColor: '#283238', color: 'white' }}>
          <div>
            <strong>{props.tooltipData.name}</strong>
          </div>
          <div style={{ marginTop: '5px', fontSize: '12px' }}>
            {props.tooltipData.max && <div>max: {secondsToDurationString(props.tooltipData.max)}</div>}
            {props.tooltipData.thirdQuartile && <div>third quartile: {secondsToDurationString(props.tooltipData.thirdQuartile)}</div>}
            {props.tooltipData.median && <div>median: {secondsToDurationString(props.tooltipData.median)}</div>}
            {props.tooltipData.average && <div>average: {secondsToDurationString(props.tooltipData.average)}</div>}
            {props.tooltipData.firstQuartile && <div>first quartile: {secondsToDurationString(props.tooltipData.firstQuartile)}</div>}
            {props.tooltipData.min && <div>min: {secondsToDurationString(props.tooltipData.min)}</div>}
          </div>
        </TooltipWithBounds>
      )}
    </div>
  )
}

export default withTooltip<StatsPlotProps, TooltipData>(DurationBoxPlot)
