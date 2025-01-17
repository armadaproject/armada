import { Card, styled, Typography, TypographyProps } from "@mui/material"

import { SPACING } from "../../../styling/spacing"

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

const StyledCard = styled(Card)(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  rowGap: theme.spacing(SPACING.sm),
  alignItems: "flex-start",
  padding: theme.spacing(SPACING.md),
}))

const CardLabel = styled(Typography)(({ theme }) => ({
  fontSize: theme.typography.h6.fontSize,
  color: theme.palette.text.secondary,
  lineHeight: 1,
}))

const CardValue = styled(Typography)(({ theme }) => ({
  fontSize: theme.typography.h4.fontSize,
  fontWeight: theme.typography.fontWeightMedium,
  lineHeight: 1,
}))

export interface SidebarTabProminentValueCardProps {
  label: string
  value: string
}

export const SidebarTabProminentValueCard = ({ label, value }: SidebarTabProminentValueCardProps) => (
  <div>
    <StyledCard variant="outlined">
      <div>
        <CardLabel>{label}</CardLabel>
      </div>
      <div>
        <CardValue>{value}</CardValue>
      </div>
    </StyledCard>
  </div>
)
