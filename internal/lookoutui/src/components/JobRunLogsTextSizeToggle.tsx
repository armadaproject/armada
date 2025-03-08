import { FC } from "react"

import { FormatSize } from "@mui/icons-material"
import { styled, SvgIconProps, ToggleButton, ToggleButtonGroup, ToggleButtonProps } from "@mui/material"

import { SPACING } from "../styling/spacing"
import { JOB_RUN_LOGS_TEXT_SIZES, JobRunLogsTextSize, useJobRunLogsTextSize } from "../userSettings"

const ToggleButtonSmall = styled(ToggleButton)(({ theme }) => ({
  display: "flex",
  gap: theme.spacing(SPACING.xs),

  [".MuiSvgIcon-root"]: {
    fontSize: 12,
  },
}))

const ToggleButtonMedium = styled(ToggleButton)(({ theme }) => ({
  display: "flex",
  gap: theme.spacing(SPACING.xs),

  [".MuiSvgIcon-root"]: {
    fontSize: 17,
  },
}))

const ToggleButtonLarge = styled(ToggleButton)(({ theme }) => ({
  display: "flex",
  gap: theme.spacing(SPACING.xs),

  [".MuiSvgIcon-root"]: {
    fontSize: 24,
  },
}))

const textSizeToggleButtonComponentMap: Record<JobRunLogsTextSize, FC<ToggleButtonProps>> = {
  small: ToggleButtonSmall,
  medium: ToggleButtonMedium,
  large: ToggleButtonLarge,
}

const textSizeIconFontSizeMap: Record<JobRunLogsTextSize, SvgIconProps["fontSize"]> = {
  small: "inherit",
  medium: "medium",
  large: "large",
}

const textSizeDisplayNameMap: Record<JobRunLogsTextSize, string> = {
  small: "Small",
  medium: "Medium",
  large: "Large",
}

export interface JobRunLogsTextSizeToggleProps {
  compact?: boolean
}

export const JobRunLogsTextSizeToggle = ({ compact }: JobRunLogsTextSizeToggleProps) => {
  const [jobRunLogsTextSize, setJobRunLogsTextSize] = useJobRunLogsTextSize()

  return (
    <ToggleButtonGroup
      color="primary"
      value={jobRunLogsTextSize}
      exclusive
      aria-label="colour mode"
      onChange={(_, newTextSize) => setJobRunLogsTextSize(newTextSize as JobRunLogsTextSize)}
      size={compact ? "small" : "medium"}
      fullWidth={!compact}
    >
      {JOB_RUN_LOGS_TEXT_SIZES.map((textSize) => {
        const ToggleButtonComponent = textSizeToggleButtonComponentMap[textSize]
        return (
          <ToggleButtonComponent key={textSize} value={textSize}>
            <FormatSize fontSize={textSizeIconFontSizeMap[textSize]} />
            {!compact && <div>{textSizeDisplayNameMap[textSize]}</div>}
          </ToggleButtonComponent>
        )
      })}
    </ToggleButtonGroup>
  )
}
