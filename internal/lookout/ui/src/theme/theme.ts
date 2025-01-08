import { createTheme } from "@mui/material"

import { MuiChip } from "./components/chip"
import { MuiTooltip } from "./components/tooltip"
import { darkModePalette, lightModePalette } from "./palette"
import { typography } from "./typography"

export const theme = createTheme({
  colorSchemes: { dark: { palette: darkModePalette }, light: { palette: lightModePalette } },
  components: { MuiChip, MuiTooltip },
  typography,
})
