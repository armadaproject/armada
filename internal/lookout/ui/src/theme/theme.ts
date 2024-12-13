import { createTheme } from "@mui/material"

import { palette } from "./palette"
import { typography } from "./typography"

export const theme = createTheme({
  colorSchemes: { dark: false, light: true },
  palette,
  typography,
})
