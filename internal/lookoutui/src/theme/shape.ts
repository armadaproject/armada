import { ShapeOptions } from "@mui/system"

import { LookoutThemeConfigOptions } from "./lookoutThemeConfig"

export const createShapeOptions = (config: Required<LookoutThemeConfigOptions>): ShapeOptions => ({
  borderRadius: config.borderRadiusPx,
})
