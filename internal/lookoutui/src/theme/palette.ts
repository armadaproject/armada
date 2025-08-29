import { darken, getContrastRatio, lighten, Palette, PaletteColorOptions, PaletteOptions } from "@mui/material"

import { LookoutThemeConfigOptions } from "./lookoutThemeConfig"

const TONAL_OFFSET = 0.3
const CONTRAST_THRESHOLD = 3

export const CUSTOM_PALETTE_COLOR_TOKENS = [
  "appBar",
  "statusQueued",
  "statusPending",
  "statusRunning",
  "statusSucceeded",
  "statusFailed",
  "statusCancelled",
  "statusPreempted",
  "statusLeased",
  "statusRejected",
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

export const createLightModePaletteOptions = (config: Required<LookoutThemeConfigOptions>): PaletteOptions => ({
  primary: augmentColor(config.primaryColour),
  secondary: augmentColor(config.secondaryColour),
  appBar: augmentColor(config.appBarColour),
  error: augmentColor(config.errorColour),
  warning: augmentColor(config.warningColour),
  info: augmentColor(config.infoColour),
  success: augmentColor(config.successColour),
  statusQueued: augmentColor(config.statusQueuedColour),
  statusPending: augmentColor(config.statusPendingColour),
  statusRunning: augmentColor(config.statusRunningColour),
  statusSucceeded: augmentColor(config.statusSucceededColour),
  statusFailed: augmentColor(config.statusFailedColour),
  statusCancelled: augmentColor(config.statusCancelledColour),
  statusPreempted: augmentColor(config.statusPreemptedColour),
  statusLeased: augmentColor(config.statusLeasedColour),
  statusRejected: augmentColor(config.statusRejectedColour),
  background: {
    default: config.defaultBackgroundColour,
    paper: config.paperSurfaceBackgroundColour,
  },
  tonalOffset: TONAL_OFFSET,
  contrastThreshold: CONTRAST_THRESHOLD,
})

export const createDarkModePaletteOptions = (config: Required<LookoutThemeConfigOptions>): PaletteOptions => ({
  primary: augmentColor(config.primaryColourDark),
  secondary: augmentColor(config.secondaryColourDark),
  appBar: augmentColor(config.appBarColourDark),
  error: augmentColor(config.errorColourDark),
  warning: augmentColor(config.warningColourDark),
  info: augmentColor(config.infoColourDark),
  success: augmentColor(config.successColourDark),
  statusQueued: augmentColor(config.statusQueuedColourDark),
  statusPending: augmentColor(config.statusPendingColourDark),
  statusRunning: augmentColor(config.statusRunningColourDark),
  statusSucceeded: augmentColor(config.statusSucceededColourDark),
  statusFailed: augmentColor(config.statusFailedColourDark),
  statusCancelled: augmentColor(config.statusCancelledColourDark),
  statusPreempted: augmentColor(config.statusPreemptedColourDark),
  statusLeased: augmentColor(config.statusLeasedColourDark),
  statusRejected: augmentColor(config.statusRejectedColourDark),
  background: {
    default: config.defaultBackgroundColourDark,
    paper: config.paperSurfaceBackgroundColourDark,
  },
  tonalOffset: TONAL_OFFSET,
  contrastThreshold: CONTRAST_THRESHOLD,
})

declare module "@mui/material/styles" {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface Palette extends CustomPaletteColorTokensPaletteMap {}
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface PaletteOptions extends CustomPaletteColorTokensPaletteOptionsMap {}
}

declare module "@mui/material/Alert" {
  interface AlertPropsColorOverrides extends CustomPaletteColorTokensTrueMap {
    secondary: true
  }
}

declare module "@mui/material/Button" {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface ButtonPropsColorOverrides extends CustomPaletteColorTokensTrueMap {}
}

declare module "@mui/material/Chip" {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface ChipPropsColorOverrides extends CustomPaletteColorTokensTrueMap {}
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface ChipClasses extends Record<`color${Capitalize<CustomPaletteColorToken>}`, string> {}
}

declare module "@mui/material/SvgIcon" {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface SvgIconPropsColorOverrides extends CustomPaletteColorTokensTrueMap {}
}
