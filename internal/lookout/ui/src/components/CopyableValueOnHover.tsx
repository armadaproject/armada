import { ReactNode, useState } from "react"

import { styled } from "@mui/material"

import { CopyIconButton, CopyIconButtonProps } from "./CopyIconButton"

const OuterContainer = styled("div")({
  display: "flex",
  flexDirection: "row",
  gap: "1ch",
})

export interface CopyableValueOnHoverProps {
  children: ReactNode
  copyContent: string
  onCopyButtonClick?: CopyIconButtonProps["onClick"]
}

export const CopyableValueOnHover = ({ children, copyContent, onCopyButtonClick }: CopyableValueOnHoverProps) => {
  const [copyIconButtonHidden, setCopyIconButtonHidden] = useState(true)
  return (
    <OuterContainer
      onMouseEnter={() => setCopyIconButtonHidden(false)}
      onMouseLeave={() => setCopyIconButtonHidden(true)}
    >
      <div>{children}</div>
      <div>
        <CopyIconButton content={copyContent} size="small" onClick={onCopyButtonClick} hidden={copyIconButtonHidden} />
      </div>
    </OuterContainer>
  )
}
