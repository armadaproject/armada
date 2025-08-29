import { ReactNode, useCallback, useEffect, useMemo } from "react"

import { ThemeProvider } from "@mui/material"

import { getConfig } from "../config"
import { createLookoutTheme, DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS } from "../theme"
import { useVisualThemeCustomConfig, useVisualThemeName } from "../userSettings"

import { SelectableThemeConfig, LookoutThemeContext, LookoutThemeContextProps } from "./LookoutThemeContext"

export const DEFAULT_LOOKOUT_UI_THEME_NAME = "__DEFAULT_LOOKOUT_THEME__"
export const CUSTOM_LOOKOUT_UI_THEME_NAME = "__CUSTOM_LOOKOUT_THEME__"

export interface LookoutThemeProviderProps {
  children: ReactNode
}

export const LookoutThemeProvider = ({ children }: LookoutThemeProviderProps) => {
  const configCustomThemeConfigs = getConfig().customThemeConfigs

  const [visualThemeName, setVisualThemeName] = useVisualThemeName()
  const [visualThemeCustomConfig] = useVisualThemeCustomConfig()

  const selectableThemeConfigs = useMemo<SelectableThemeConfig[]>(
    () => [
      {
        name: DEFAULT_LOOKOUT_UI_THEME_NAME,
        displayName: "Default Armada Lookout",
        themeConfig: DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS,
      },
      ...(configCustomThemeConfigs?.themes ?? []).map(({ name, themeConfig }) => ({
        name,
        displayName: name,
        themeConfig,
      })),
      {
        name: CUSTOM_LOOKOUT_UI_THEME_NAME,
        displayName: "Custom",
        themeConfig: visualThemeCustomConfig,
      },
    ],
    [visualThemeCustomConfig],
  )

  useEffect(() => {
    if (visualThemeName === "") {
      const isConfigDefaultThemeNameValid =
        configCustomThemeConfigs?.defaultThemeName &&
        configCustomThemeConfigs?.themes.find(({ name }) => name === configCustomThemeConfigs.defaultThemeName)

      setVisualThemeName(
        isConfigDefaultThemeNameValid ? configCustomThemeConfigs.defaultThemeName : DEFAULT_LOOKOUT_UI_THEME_NAME,
      )
    }
  }, [visualThemeName, setVisualThemeName])

  const selectedThemeConfigIndex = useMemo(() => {
    if (visualThemeName === "") {
      return 0
    }

    const foundIndex = selectableThemeConfigs.findIndex(({ name }) => name === visualThemeName)
    if (foundIndex === -1) {
      return 0
    }

    return foundIndex
  }, [visualThemeName, selectableThemeConfigs])

  const setSelectedThemeConfigIndex = useCallback(
    (themeConfigIndex: number) => {
      const newVisualThemeName = selectableThemeConfigs[themeConfigIndex]?.name
      if (newVisualThemeName) {
        setVisualThemeName(newVisualThemeName)
      }
    },
    [selectableThemeConfigs, setVisualThemeName],
  )

  const contextValue = useMemo<LookoutThemeContextProps>(
    () => ({
      selectedThemeConfigIndex,
      setSelectedThemeConfigIndex,
      selectableThemeConfigs,
    }),
    [selectedThemeConfigIndex, setSelectedThemeConfigIndex, selectableThemeConfigs],
  )

  const lookoutTheme = useMemo(() => {
    return createLookoutTheme(selectableThemeConfigs[selectedThemeConfigIndex].themeConfig)
  }, [selectedThemeConfigIndex, selectableThemeConfigs])

  return (
    <LookoutThemeContext.Provider value={contextValue}>
      <ThemeProvider theme={lookoutTheme} defaultMode="light">
        {children}
      </ThemeProvider>
    </LookoutThemeContext.Provider>
  )
}
