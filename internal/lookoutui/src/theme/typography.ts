import { type TypographyVariantsOptions } from "@mui/material/styles"

import { LookoutThemeConfigOptions } from "./lookoutThemeConfig"

const DEFAULT_FALLBACK_FONT_FAMILY = ["Arial", "sans-serif"].map((f) => `'${f}'`).join(",")
const DEFAULT_FALLBACK_MONOSPACE_FONT_FAMILY = ["Courier New", "monospace"].map((f) => `'${f}'`).join(",")

export const createTypographyOptions = (config: Required<LookoutThemeConfigOptions>): TypographyVariantsOptions => ({
  fontFamily: [config.fontFamily, DEFAULT_FALLBACK_FONT_FAMILY].join(","),
  h1: {
    fontSize: "3rem",
    lineHeight: 1.2,
    fontWeight: 300,
  },
  h2: {
    fontSize: "2.5rem",
    lineHeight: 1.25,
    fontWeight: 300,
  },
  h3: {
    fontSize: "2rem",
    lineHeight: 1.3,
    fontWeight: 400,
  },
  h4: {
    fontSize: "1.75rem",
    lineHeight: 1.3,
    fontWeight: 500,
  },
  h5: {
    fontSize: "1.5rem",
    lineHeight: 1.35,
    fontWeight: 500,
  },
  h6: {
    fontSize: "1.25rem",
    lineHeight: 1.4,
    fontWeight: 500,
  },
  button: { textTransform: config.uppercaseButtonText ? "uppercase" : "none" },
  overline: { textTransform: config.uppercaseOverlineText ? "uppercase" : "none" },
})

export const createMonospaceFontFamily = (config: Required<LookoutThemeConfigOptions>) =>
  [config.monospaceFontFamily, DEFAULT_FALLBACK_MONOSPACE_FONT_FAMILY].join(", ")
