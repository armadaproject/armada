import { TypographyOptions } from "@mui/material/styles/createTypography"

export const typography: TypographyOptions = {
  fontFamily: [
    "-apple-system",
    "BlinkMacSystemFont",
    "'Segoe UI'",
    "'Roboto'",
    "'Oxygen'",
    "'Ubuntu'",
    "'Cantarell'",
    "'Fira Sans'",
    "'Droid Sans'",
    "'Helvetica Neue'",
    "sans-serif",
  ].join(","),
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
}
