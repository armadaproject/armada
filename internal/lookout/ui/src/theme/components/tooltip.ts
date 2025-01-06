import { Components, tooltipClasses } from "@mui/material"

export const MuiTooltip: Components["MuiTooltip"] = {
  defaultProps: {
    arrow: true,
    placement: "top",
    slotProps: {
      popper: {
        sx: {
          [`&.${tooltipClasses.popper}[data-popper-placement*="bottom"] .${tooltipClasses.tooltip}`]: {
            marginTop: "5px",
          },
          [`&.${tooltipClasses.popper}[data-popper-placement*="top"] .${tooltipClasses.tooltip}`]: {
            marginBottom: "5px",
          },
          [`&.${tooltipClasses.popper}[data-popper-placement*="right"] .${tooltipClasses.tooltip}`]: {
            marginLeft: "5px",
          },
          [`&.${tooltipClasses.popper}[data-popper-placement*="left"] .${tooltipClasses.tooltip}`]: {
            marginRight: "5px",
          },
        },
      },
    },
  },
}
