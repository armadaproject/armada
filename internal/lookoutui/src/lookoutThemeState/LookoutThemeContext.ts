import { createContext } from "react"

import { LookoutThemeConfigOptions } from "../theme"

export interface SelectableThemeConfig {
  name: string
  displayName: string
  themeConfig: LookoutThemeConfigOptions
}

export interface LookoutThemeContextProps {
  selectedThemeConfigIndex: number
  setSelectedThemeConfigIndex: (themeConfigIndex: number) => void
  selectableThemeConfigs: SelectableThemeConfig[]
}

export const LookoutThemeContext = createContext<LookoutThemeContextProps | undefined>(undefined)
