import { Add, FilterAlt } from "@mui/icons-material"
import { SvgIcon, SvgIconProps } from "@mui/material"

export const AddFilter: typeof SvgIcon = (props: SvgIconProps) => (
  <SvgIcon {...props}>
    <Add {...props} viewBox="-20 -20 44 44" />
    <FilterAlt {...props} viewBox="0 0 28 28" />
  </SvgIcon>
)

AddFilter.muiName = "AddFilter"
