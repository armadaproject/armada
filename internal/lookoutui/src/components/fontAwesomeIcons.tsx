import { forwardRef } from "react"

import type { IconDefinition } from "@fortawesome/fontawesome-common-types"
import {
  faBan,
  faCheckCircle,
  faExchangeAlt,
  faFileContract,
  faHand,
  faHourglassHalf,
  faPlayCircle,
  faSpinner,
  faTimesCircle,
} from "@fortawesome/free-solid-svg-icons"
import { SvgIcon, SvgIconProps } from "@mui/material"

import "@fortawesome/fontawesome-svg-core/styles.css"

export interface FontAwesomeSvgIconProps extends SvgIconProps {
  icon: IconDefinition
}

export const FontAwesomeSvgIcon = forwardRef<SVGSVGElement, FontAwesomeSvgIconProps>(
  ({ icon, ...svgIconProps }, ref) => {
    const {
      icon: [width, height, , , svgPathData],
    } = icon

    return (
      <SvgIcon ref={ref} viewBox={`0 0 ${width} ${height}`} {...svgIconProps}>
        {typeof svgPathData === "string" ? (
          <path d={svgPathData} />
        ) : (
          /**
           * A multi-path Font Awesome icon seems to imply a duotune icon. The 0th path seems to
           * be the faded element (referred to as the "secondary" path in the Font Awesome docs)
           * of a duotone icon. 40% is the default opacity.
           *
           * @see https://fontawesome.com/how-to-use/on-the-web/styling/duotone-icons#changing-opacity
           */
          svgPathData.map((d: string, i: number) => <path key={i} style={{ opacity: i === 0 ? 0.4 : 1 }} d={d} />)
        )}
      </SvgIcon>
    )
  },
)

export const FaHourglassHalf = (svgIconProps: SvgIconProps) => (
  <FontAwesomeSvgIcon icon={faHourglassHalf} {...svgIconProps} />
)
export const FaSpinner = (svgIconProps: SvgIconProps) => <FontAwesomeSvgIcon icon={faSpinner} {...svgIconProps} />
export const FaPlayCircle = (svgIconProps: SvgIconProps) => <FontAwesomeSvgIcon icon={faPlayCircle} {...svgIconProps} />
export const FaCheckCircle = (svgIconProps: SvgIconProps) => (
  <FontAwesomeSvgIcon icon={faCheckCircle} {...svgIconProps} />
)
export const FaTimesCircle = (svgIconProps: SvgIconProps) => (
  <FontAwesomeSvgIcon icon={faTimesCircle} {...svgIconProps} />
)
export const FaBan = (svgIconProps: SvgIconProps) => <FontAwesomeSvgIcon icon={faBan} {...svgIconProps} />
export const FaExcahngeAlt = (svgIconProps: SvgIconProps) => (
  <FontAwesomeSvgIcon icon={faExchangeAlt} {...svgIconProps} />
)
export const FaFileContract = (svgIconProps: SvgIconProps) => (
  <FontAwesomeSvgIcon icon={faFileContract} {...svgIconProps} />
)
export const FaHand = (svgIconProps: SvgIconProps) => <FontAwesomeSvgIcon icon={faHand} {...svgIconProps} />
