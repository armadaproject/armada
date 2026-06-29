import { useState } from "react"

import { ContentCopy } from "@mui/icons-material"
import { IconButton, IconButtonProps, styled, SvgIcon, Tooltip } from "@mui/material"

import { useCustomSnackbar } from "./hooks/useCustomSnackbar"

const LEAVE_DELAY_MS = 1_000

const StyledIconButton = styled(IconButton)<IconButtonProps & { hidden: boolean }>(({ hidden }) => ({
  visibility: hidden ? "hidden" : "unset",
}))

export interface CopyIconButtonProps {
  content: string
  size?: IconButtonProps["size"]
  onClick?: IconButtonProps["onClick"]
  hidden?: boolean
  Icon?: typeof SvgIcon
  copiedTooltipTitle?: string
}

export const CopyIconButton = ({
  content,
  size,
  onClick,
  hidden = false,
  Icon = ContentCopy,
  copiedTooltipTitle = "Copied!",
}: CopyIconButtonProps) => {
  const [tooltipOpen, setTooltipOpen] = useState(false)
  const openSnackbar = useCustomSnackbar()

  return (
    <Tooltip
      title={copiedTooltipTitle}
      onClose={() => setTooltipOpen(false)}
      open={tooltipOpen}
      leaveDelay={LEAVE_DELAY_MS}
      arrow={false}
    >
      <StyledIconButton
        size={size}
        onClick={async (e) => {
          onClick?.(e)
          try {
            await navigator.clipboard.writeText(content)
            setTooltipOpen(true)
          } catch (error) {
            openSnackbar(`Failed to copy to clipboard: ${error}`, "error")
          }
        }}
        aria-label="copy"
        hidden={hidden && !tooltipOpen}
      >
        <Icon fontSize="inherit" />
      </StyledIconButton>
    </Tooltip>
  )
}
