import { useContext } from "react"

import { LookoutThemeContext } from "./LookoutThemeContext"

export const useLookoutThemeConfigState = () => {
  const context = useContext(LookoutThemeContext)
  if (context === undefined) {
    throw new Error("useLookoutThemeConfigState() must be used within a LookoutThemeProvider")
  }
  return context
}
