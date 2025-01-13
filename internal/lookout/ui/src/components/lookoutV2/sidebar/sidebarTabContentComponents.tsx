import { styled, Typography, TypographyProps } from "@mui/material"

const StyledHeading = styled(Typography)(({ theme }) => ({
  fontSize: theme.typography.h6.fontSize,
  fontWeight: theme.typography.fontWeightMedium,
}))

export const SidebarTabHeading = (props: TypographyProps) => <StyledHeading component="h4" {...props} />

const StyledSubheading = styled(Typography)(({ theme }) => ({
  fontSize: theme.typography.subtitle1.fontSize,
  fontWeight: theme.typography.fontWeightMedium,
}))

export const SidebarTabSubheading = (props: TypographyProps) => <StyledSubheading component="h5" {...props} />
