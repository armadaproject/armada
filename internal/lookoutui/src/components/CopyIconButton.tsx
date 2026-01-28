import { useState } from "react"

import { ContentCopy } from "@mui/icons-material"
import { IconButton, IconButtonProps, styled, SvgIcon, Tooltip } from "@mui/material"

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
        onClick={(e) => {
          onClick?.(e)
          navigator.clipboard.writeText(content)
          setTooltipOpen(true)
        }}
        aria-label="copy"
        hidden={hidden && !tooltipOpen}
      >
        <Icon fontSize="inherit" />
      </StyledIconButton>
    </Tooltip>
  )
}
