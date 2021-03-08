import React from "react";
import { AxisTop } from '@visx/axis';
import { Padding, secondsToDurationString } from "../../utils";
import { scaleLinear } from "@visx/scale";

type DurationAxisProps = {
  height: number
  width: number
  padding: Padding
  minValue: number
  maxValue: number
  nTicks: number
}

export default function DurationAxis(props: DurationAxisProps) {
  const scale = scaleLinear<number>({
    range: [props.padding.left, props.width - props.padding.right],
    round: true,
    domain: [props.minValue, props.maxValue],
  })

  return (
    <svg height={props.height} width={props.width}>
      <AxisTop
        top={props.height - 1}
        scale={scale}
        numTicks={10}
        tickFormat={(v) => secondsToDurationString(v.valueOf())}
        tickLabelProps={() => ({
          dy: "-0.75em",
          textAnchor: "middle",
          fontSize: 12,
        })}/>
    </svg>
  )
}
