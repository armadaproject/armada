import { ExpandMore } from "@mui/icons-material"
import { Components, Theme } from "@mui/material"

export const MuiAccordion: Components<Theme>["MuiAccordion"] = {
  defaultProps: { disableGutters: true, variant: "outlined" },
  styleOverrides: {
    root: ({ theme }) => ({
      border: `1px solid ${theme.palette.divider}`,

      "&:not(:last-child)": {
        borderBottom: 0,
      },

      "&::before": {
        display: "none",
      },
    }),
  },
}

export const MuiAccordionSummary: Components<Theme>["MuiAccordionSummary"] = {
  defaultProps: { expandIcon: <ExpandMore /> },
}

export const MuiAccordionDetails: Components<Theme>["MuiAccordionDetails"] = {
  styleOverrides: {
    root: ({ theme }) => ({
      borderTop: `0.5px solid ${theme.palette.divider}`,
    }),
  },
}
