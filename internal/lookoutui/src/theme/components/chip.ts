import { alpha, ChipClasses, ChipProps, Components, Theme } from "@mui/material"

declare module "@mui/material/Chip" {
  interface ChipPropsVariantOverrides {
    shaded: true
  }
}

type ChipColor = Exclude<ChipProps["color"], undefined>

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

              [`&.MuiChip-${chipClassesKey}`]: [
                {
                  backgroundColor: color === "default" ? undefined : alpha(theme.palette[color].light, 0.12),
                  color: color === "default" ? theme.palette.grey[400] : theme.palette[color].dark,
                },
                theme.applyStyles("dark", {
                  backgroundColor: color === "default" ? undefined : alpha(theme.palette[color].dark, 0.12),
                  color: color === "default" ? theme.palette.grey[700] : theme.palette[color].light,
                }),
              ],
            }
          }, {}),
        },
      ],
    },
  ],
}
