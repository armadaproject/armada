import { createTheme, SupportedColorScheme } from "@mui/material"

import { MuiAccordion, MuiAccordionDetails, MuiAccordionSummary } from "./components/accordion"
import { MuiChip } from "./components/chip"
import { MuiTooltip } from "./components/tooltip"
import { LookoutThemeConfigOptions, mergeLookoutThemeConfigWithDefaults } from "./lookoutThemeConfig"
import { createDarkModePaletteOptions, createLightModePaletteOptions } from "./palette"
import { createShapeOptions } from "./shape"
import { createMonospaceFontFamily, createTypographyOptions } from "./typography"

export const createLookoutTheme = (config: LookoutThemeConfigOptions, colourMode?: SupportedColorScheme) => {
  const mergedConfig = mergeLookoutThemeConfigWithDefaults(config)

  const components = {
    MuiAccordion,
    MuiAccordionDetails,
    MuiAccordionSummary,
    MuiChip,
    MuiTooltip,
    MuiCssBaseline: {
      styleOverrides: {
        code: {
          fontFamily: createMonospaceFontFamily(mergedConfig),
        },
      },
    },
  }
  const shape = createShapeOptions(mergedConfig)
  const typography = createTypographyOptions(mergedConfig)

  if (colourMode) {
    return createTheme({
      palette: {
        mode: colourMode,
        ...(colourMode === "dark"
          ? createDarkModePaletteOptions(mergedConfig)
          : createLightModePaletteOptions(mergedConfig)),
      },
      components,
      shape,
      typography,
    })
  }

  return createTheme({
    colorSchemes: {
      dark: { palette: createDarkModePaletteOptions(mergedConfig) },
      light: { palette: createLightModePaletteOptions(mergedConfig) },
    },
    components,
    shape,
    typography,
  })
}
