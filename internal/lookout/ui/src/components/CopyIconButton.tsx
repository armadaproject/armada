import { useState } from "react"

import { ContentCopy } from "@mui/icons-material"
import { IconButton, IconButtonProps, styled, Tooltip } from "@mui/material"

const LEAVE_DELAY_MS = 1_000

const StyledIconButton = styled(IconButton)<IconButtonProps & { hidden: boolean }>(({ hidden }) => ({
  padding: 0,
  visibility: hidden ? "hidden" : "unset",
}))

export interface CopyIconButtonProps {
  content: string
  size?: IconButtonProps["size"]
  onClick?: IconButtonProps["onClick"]
  hidden?: boolean
}

export const CopyIconButton = ({ content, size, onClick, hidden = false }: CopyIconButtonProps) => {
  const [tooltipOpen, setTooltipOpen] = useState(false)

  return (
    <Tooltip title="Copied!" onClose={() => setTooltipOpen(false)} open={tooltipOpen} leaveDelay={LEAVE_DELAY_MS}>
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
        <ContentCopy fontSize="inherit" />
      </StyledIconButton>
    </Tooltip>
  )
}
