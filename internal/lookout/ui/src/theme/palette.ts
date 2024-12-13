import { darken, getContrastRatio, lighten, Palette, PaletteColorOptions, PaletteOptions } from "@mui/material"

const TONAL_OFFSET = 0.3
const CONTRAST_THRESHOLD = 3

// Brand colours
export const LIGHT_BLUE = "#25AEFF" as const
export const ORANGE = "#FF7625" as const // the complementary colour to LIGHT_BLUE

// Status colours
export const STATUS_GREY = "#6C757D" as const
export const STATUS_BLUE = "#007BFF" as const
export const STATUS_GREEN = "#28A745" as const
export const STATUS_AMBER = "#FFC107" as const
export const STATUS_RED = "#88022B" as const

export const CUSTOM_PALETTE_COLOR_TOKENS = [
  "statusGrey",
  "statusBlue",
  "statusGreen",
  "statusAmber",
  "statusRed",
] as const

export type CustomPaletteColorToken = (typeof CUSTOM_PALETTE_COLOR_TOKENS)[number]

type CustomPaletteColorTokensPaletteMap = { [Name in CustomPaletteColorToken]: Palette["primary"] }
type CustomPaletteColorTokensPaletteOptionsMap = {
  [Name in CustomPaletteColorToken]: PaletteOptions["primary"]
}
type CustomPaletteColorTokensTrueMap = { [Name in CustomPaletteColorToken]: true }

const augmentColor = (main: string): PaletteColorOptions => ({
  light: lighten(main, TONAL_OFFSET),
  main,
  dark: darken(main, TONAL_OFFSET),
  contrastText: getContrastRatio(main, "#fff") > CONTRAST_THRESHOLD ? "#fff" : "#000",
})

export const palette: PaletteOptions = {
  primary: { main: LIGHT_BLUE, contrastText: "#FFF" },
  secondary: { main: ORANGE },
  statusGrey: augmentColor(STATUS_GREY),
  statusBlue: augmentColor(STATUS_BLUE),
  statusGreen: augmentColor(STATUS_GREEN),
  statusAmber: augmentColor(STATUS_AMBER),
  statusRed: augmentColor(STATUS_RED),
  tonalOffset: TONAL_OFFSET,
  contrastThreshold: CONTRAST_THRESHOLD,
}

declare module "@mui/material/styles" {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface
  interface Palette extends CustomPaletteColorTokensPaletteMap {}
  // eslint-disable-next-line @typescript-eslint/no-empty-interface
  interface PaletteOptions extends CustomPaletteColorTokensPaletteOptionsMap {}
}

declare module "@mui/material/Alert" {
  interface AlertPropsColorOverrides extends CustomPaletteColorTokensTrueMap {
    secondary: true
  }
}

declare module "@mui/material/Button" {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface
  interface ButtonPropsColorOverrides extends CustomPaletteColorTokensTrueMap {}
}

declare module "@mui/material/Chip" {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface
  interface ChipPropsColorOverrides extends CustomPaletteColorTokensTrueMap {}
}

declare module "@mui/material/SvgIcon" {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface
  interface SvgIconPropsColorOverrides extends CustomPaletteColorTokensTrueMap {}
}
