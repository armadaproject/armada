import { getContrastRatio } from "@mui/material"

export const getContrastText = (backgroundColor: string) =>
  getContrastRatio(backgroundColor || "#fff", "#fff") > 4.5 ? "#fff" : "#000"
