import { alpha, ChipClasses, ChipProps, Components, Theme } from "@mui/material"

declare module "@mui/material/Chip" {
  interface ChipPropsVariantOverrides {
    shaded: true
  }
}

type ChipColor = Exclude<ChipProps["color"], undefined>

const getShadedChipTextColor = (color: ChipColor, theme: Theme): string => {
  switch (theme.palette.mode) {
    case "light":
      return color === "default" ? theme.palette.grey[400] : theme.palette[color].dark
    case "dark":
      return color === "default" ? theme.palette.grey[700] : theme.palette[color].light
  }
}

const getShadedChipBackgroundColor = (color: ChipColor, theme: Theme): string | undefined => {
  switch (theme.palette.mode) {
    case "light":
      return color === "default" ? undefined : alpha(theme.palette[color].light, 0.12)
    case "dark":
      return color === "default" ? undefined : alpha(theme.palette[color].dark, 0.12)
  }
}

const ChipColorClassesKeyMap: Record<ChipColor, keyof ChipClasses> = {
  default: "colorDefault",
  primary: "colorPrimary",
  secondary: "colorSecondary",
  error: "colorError",
  info: "colorInfo",
  success: "colorSuccess",
  warning: "colorWarning",
  statusGrey: "colorStatusGrey",
  statusBlue: "colorStatusBlue",
  statusGreen: "colorStatusGreen",
  statusAmber: "colorStatusAmber",
  statusRed: "colorStatusRed",
}

export const MuiChip: Components<Theme>["MuiChip"] = {
  variants: [
    {
      props: { variant: "shaded" },
      style: ({ theme }) => [
        {
          fontWeight: theme.typography.fontWeightMedium,

          ...Object.entries(ChipColorClassesKeyMap).reduce((acc, [_color, chipClassesKey]) => {
            const color = _color as ChipColor
            return {
              ...acc,

              [`&.MuiChip-${chipClassesKey}`]: {
                backgroundColor: getShadedChipBackgroundColor(color, theme),
                color: getShadedChipTextColor(color, theme),
              },
            }
          }, {}),
        },
      ],
    },
  ],
}
