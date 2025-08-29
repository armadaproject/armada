import { useEffect, useState } from "react"

import { Square } from "@mui/icons-material"
import { ButtonBase, Popover, styled } from "@mui/material"
import { ColorPicker, ColorService, useColor } from "react-color-palette"
import "react-color-palette/css"

const StyledButton = styled(ButtonBase)(({ theme }) => ({
  borderColor: theme.palette.divider,
  borderStyle: "solid",
  borderWidth: 1,
}))

export interface HexColourPickerProps {
  value: string
  onChange: (newColour: string) => void
}

export const HexColourPicker = ({ value, onChange }: HexColourPickerProps) => {
  const [anchorEl, setAnchorEl] = useState<HTMLButtonElement | null>(null)
  const [colour, setColour] = useColor(value)

  useEffect(() => {
    setColour(ColorService.convert("hex", value))
  }, [value])

  return (
    <div>
      <StyledButton onClick={({ currentTarget }) => setAnchorEl(currentTarget)}>
        <Square fontSize="large" htmlColor={colour.hex} />
      </StyledButton>
      <Popover
        open={Boolean(anchorEl)}
        anchorEl={anchorEl}
        onClose={() => {
          onChange(colour.hex)
          setAnchorEl(null)
        }}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "right",
        }}
      >
        <ColorPicker hideInput={["rgb", "hsv"]} color={colour} onChange={setColour} />
      </Popover>
    </div>
  )
}
