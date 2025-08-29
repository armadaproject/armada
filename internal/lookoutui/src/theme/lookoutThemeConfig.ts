export interface LookoutThemeConfigOptions {
  // Typography
  fontFamily?: string
  monospaceFontFamily?: string
  uppercaseButtonText?: boolean
  uppercaseOverlineText?: boolean

  // Shape
  borderRadiusPx?: number

  // Palette (light)
  primaryColour?: string
  secondaryColour?: string
  appBarColour?: string
  errorColour?: string
  warningColour?: string
  infoColour?: string
  successColour?: string
  statusQueuedColour?: string
  statusPendingColour?: string
  statusRunningColour?: string
  statusSucceededColour?: string
  statusFailedColour?: string
  statusCancelledColour?: string
  statusPreemptedColour?: string
  statusLeasedColour?: string
  statusRejectedColour?: string
  defaultBackgroundColour?: string
  paperSurfaceBackgroundColour?: string

  // Palette (dark)
  primaryColourDark?: string
  secondaryColourDark?: string
  appBarColourDark?: string
  errorColourDark?: string
  warningColourDark?: string
  infoColourDark?: string
  successColourDark?: string
  statusQueuedColourDark?: string
  statusPendingColourDark?: string
  statusRunningColourDark?: string
  statusSucceededColourDark?: string
  statusFailedColourDark?: string
  statusCancelledColourDark?: string
  statusPreemptedColourDark?: string
  statusLeasedColourDark?: string
  statusRejectedColourDark?: string
  defaultBackgroundColourDark?: string
  paperSurfaceBackgroundColourDark?: string
}

export const DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS: Required<LookoutThemeConfigOptions> = {
  fontFamily: [
    "-apple-system",
    "BlinkMacSystemFont",
    "Segoe UI",
    "Roboto",
    "Oxygen",
    "Ubuntu",
    "Cantarell",
    "Fira Sans",
    "Droid Sans",
    "Helvetica Neue",
  ]
    .map((f) => `'${f}'`)
    .join(", "),
  monospaceFontFamily: ["Source Code Pro", "Menlo", "Monaco", "Consolas"].map((f) => `'${f}'`).join(", "),
  uppercaseButtonText: true,
  uppercaseOverlineText: true,

  borderRadiusPx: 4,

  primaryColour: "#1A8EF0", // light blue
  secondaryColour: "#FF7625", // orange
  appBarColour: "#1A8EF0",
  errorColour: "#D32F2F",
  warningColour: "#ED6C02",
  infoColour: "#0288D1",
  successColour: "#2E7D32",
  statusQueuedColour: "#6C757D",
  statusPendingColour: "#6C757D",
  statusRunningColour: "#007BFF",
  statusSucceededColour: "#28A745",
  statusFailedColour: "#88022B",
  statusCancelledColour: "#FFC107",
  statusPreemptedColour: "#FFC107",
  statusLeasedColour: "#6C757D",
  statusRejectedColour: "#88022B",
  defaultBackgroundColour: "#FFFFFF",
  paperSurfaceBackgroundColour: "#FFFFFF",

  primaryColourDark: "#1A8EF0", // light blue
  secondaryColourDark: "#FF7625", // orange
  appBarColourDark: "#222222",
  errorColourDark: "#F44336",
  warningColourDark: "#FFA726",
  infoColourDark: "#29B6F6",
  successColourDark: "#66BB6A",
  statusQueuedColourDark: "#6C757D",
  statusPendingColourDark: "#6C757D",
  statusRunningColourDark: "#007BFF",
  statusSucceededColourDark: "#28A745",
  statusFailedColourDark: "#88022B",
  statusCancelledColourDark: "#FFC107",
  statusPreemptedColourDark: "#FFC107",
  statusLeasedColourDark: "#6C757D",
  statusRejectedColourDark: "#88022B",
  defaultBackgroundColourDark: "#121212",
  paperSurfaceBackgroundColourDark: "#121212",
}

const colourMergeKeys = [
  ["primaryColourDark", "primaryColour"],
  ["secondaryColourDark", "secondaryColour"],
  ["appBarColourDark", "appBarColour"],
  ["errorColourDark", "errorColour"],
  ["warningColourDark", "warningColour"],
  ["infoColourDark", "infoColour"],
  ["successColourDark", "successColour"],
  ["statusQueuedColourDark", "statusQueuedColour"],
  ["statusPendingColourDark", "statusPendingColour"],
  ["statusRunningColourDark", "statusRunningColour"],
  ["statusSucceededColourDark", "statusSucceededColour"],
  ["statusFailedColourDark", "statusFailedColour"],
  ["statusCancelledColourDark", "statusCancelledColour"],
  ["statusPreemptedColourDark", "statusPreemptedColour"],
  ["statusLeasedColourDark", "statusLeasedColour"],
  ["statusRejectedColourDark", "statusRejectedColour"],
] as const

export const mergeLookoutThemeConfigWithDefaults = (
  config: LookoutThemeConfigOptions,
): Required<LookoutThemeConfigOptions> => {
  // First, merge light theme colours onto dark theme colours
  const configWithMergedColours: LookoutThemeConfigOptions = { ...config }
  colourMergeKeys.forEach(([darkKey, lightKey]) => {
    const value = config[darkKey] ?? config[lightKey]
    if (value) {
      configWithMergedColours[darkKey] = value
    }
  })

  // Then, merge default config onto input config
  return {
    ...DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS,
    ...configWithMergedColours,
  }
}

export const lookoutThemeConfigOptionsToJSON = (config: LookoutThemeConfigOptions) => JSON.stringify(config)

type PrimitiveTypeString<T> = T extends string
  ? "string"
  : T extends number
    ? "number"
    : T extends boolean
      ? "boolean"
      : never

const lookoutThemeConfigOptionsSchema = {
  // Typography
  fontFamily: "string",
  monospaceFontFamily: "string",
  uppercaseButtonText: "boolean",
  uppercaseOverlineText: "boolean",

  // Shape
  borderRadiusPx: "number",

  // Palette (light, default)
  primaryColour: "string",
  secondaryColour: "string",
  appBarColour: "string",
  errorColour: "string",
  warningColour: "string",
  infoColour: "string",
  successColour: "string",
  statusQueuedColour: "string",
  statusPendingColour: "string",
  statusRunningColour: "string",
  statusSucceededColour: "string",
  statusFailedColour: "string",
  statusCancelledColour: "string",
  statusPreemptedColour: "string",
  statusLeasedColour: "string",
  statusRejectedColour: "string",
  defaultBackgroundColour: "string",
  paperSurfaceBackgroundColour: "string",

  // Palette (dark)
  primaryColourDark: "string",
  secondaryColourDark: "string",
  appBarColourDark: "string",
  errorColourDark: "string",
  warningColourDark: "string",
  infoColourDark: "string",
  successColourDark: "string",
  statusQueuedColourDark: "string",
  statusPendingColourDark: "string",
  statusRunningColourDark: "string",
  statusSucceededColourDark: "string",
  statusFailedColourDark: "string",
  statusCancelledColourDark: "string",
  statusPreemptedColourDark: "string",
  statusLeasedColourDark: "string",
  statusRejectedColourDark: "string",
  defaultBackgroundColourDark: "string",
  paperSurfaceBackgroundColourDark: "string",
} as const satisfies {
  [K in keyof LookoutThemeConfigOptions]-?: PrimitiveTypeString<NonNullable<LookoutThemeConfigOptions[K]>>
}

export const lookoutThemeConfigOptionsFromJSON = (jsonString: string | null): LookoutThemeConfigOptions => {
  if (jsonString === null) {
    return {}
  }

  try {
    const obj = JSON.parse(jsonString)

    if (typeof obj !== "object" || obj === null || Array.isArray(obj)) {
      return {}
    }

    const configOptions: Partial<LookoutThemeConfigOptions> = {}
    for (const key in lookoutThemeConfigOptionsSchema) {
      const value = obj[key as string]
      // Capture K so TypeScript can keep key/value correlated
      ;(<K extends keyof LookoutThemeConfigOptions>(k: K, v: unknown) => {
        if (typeof v === lookoutThemeConfigOptionsSchema[k]) {
          configOptions[k] = v as LookoutThemeConfigOptions[K]
        }
      })(key as keyof LookoutThemeConfigOptions, value)
    }
    return configOptions
  } catch {
    return {}
  }
}
