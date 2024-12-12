import { getContrastRatio } from "@mui/material"

export const getContrastText = (backgroundColor: string) =>
  getContrastRatio(backgroundColor || "#fff", "#fff") > 3 ? "#fff" : "#000"
