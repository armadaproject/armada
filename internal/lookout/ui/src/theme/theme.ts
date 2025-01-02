import { createTheme } from "@mui/material"

import { darkModePalette, lightModePalette } from "./palette"
import { typography } from "./typography"

export const theme = createTheme({
  colorSchemes: { dark: { palette: darkModePalette }, light: { palette: lightModePalette } },
  typography,
})
