import { ReactNode, useState } from "react"

import { IconButton, IconButtonProps, styled } from "@mui/material"

import { CopyIconButton } from "./CopyIconButton"
import { AddFilter } from "./icons"

const OuterContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  alignItems: "center",
  gap: "0.5ch",
})

const StyledIconButton = styled(IconButton)<IconButtonProps & { hidden: boolean }>(({ hidden }) => ({
  visibility: hidden ? "hidden" : "unset",
}))

export interface CopyActionProps {
  copyContent: string
}

export interface FilterActionProps {
  onFilter: () => void
}

export interface ActionableValueOnHoverProps {
  children: ReactNode
  copyAction?: CopyActionProps
  filterAction?: FilterActionProps
  stopPropogationOnActionClick?: boolean
}

export const ActionableValueOnHover = ({
  children,
  copyAction,
  filterAction,
  stopPropogationOnActionClick = false,
}: ActionableValueOnHoverProps) => {
  const [hovering, setHovering] = useState(false)
  return (
    <OuterContainer onMouseEnter={() => setHovering(true)} onMouseLeave={() => setHovering(false)}>
      <div>{children}</div>
      {copyAction && (
        <div>
          <CopyIconButton
            content={copyAction.copyContent}
            size="small"
            onClick={(e) => {
              if (stopPropogationOnActionClick) {
                e.stopPropagation()
              }
            }}
            hidden={!hovering}
          />
        </div>
      )}
      {filterAction && (
        <div>
          <StyledIconButton
            size="small"
            hidden={!hovering}
            onClick={(e) => {
              if (stopPropogationOnActionClick) {
                e.stopPropagation()
              }
              filterAction.onFilter()
            }}
          >
            <AddFilter fontSize="inherit" />
          </StyledIconButton>
        </div>
      )}
    </OuterContainer>
  )
}
