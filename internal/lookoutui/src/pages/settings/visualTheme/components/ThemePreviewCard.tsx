import { Card, CardActionArea, CardContent, Chip, Divider, Grid2, Stack, styled, Typography } from "@mui/material"

import { SPACING } from "../../../../common/spacing"
import { LookoutThemeConfigOptions } from "../../../../theme"

import { ThemePreview } from "./ThemePreview"

const StyledCard = styled(Card, { shouldForwardProp: (prop) => prop !== "selected" })<{ selected: boolean }>(
  ({ selected, theme }) => ({
    borderColor: selected ? theme.palette.primary.main : undefined,
  }),
)

const StyledDivider = styled(Divider, { shouldForwardProp: (prop) => prop !== "selected" })<{ selected: boolean }>(
  ({ selected, theme }) => ({
    borderColor: selected ? theme.palette.primary.main : undefined,
  }),
)

export interface ThemePreviewCardProps {
  displayName: string
  onClick: () => void
  selected: boolean
  customTheme: boolean
  themeConfig: LookoutThemeConfigOptions
}

export const ThemePreviewCard = ({
  displayName,
  onClick,
  selected,
  themeConfig,
  customTheme,
}: ThemePreviewCardProps) => (
  <StyledCard variant="outlined" selected={selected}>
    <CardActionArea onClick={onClick}>
      {!customTheme && (
        <>
          <CardContent>
            <Stack direction="row" spacing={SPACING.md} alignItems="center">
              <Typography component="div" variant="h6">
                {displayName}
              </Typography>
              {selected && (
                <div>
                  <Chip label="Selected" variant="outlined" color="primary" />
                </div>
              )}
            </Stack>
          </CardContent>
          <StyledDivider variant="fullWidth" selected={selected} />
        </>
      )}
      <CardContent>
        <Grid2 container spacing={SPACING.sm}>
          <Grid2 size={{ xs: 12, lg: 6 }}>
            <ThemePreview themeConfig={themeConfig} colourMode="light" />
          </Grid2>
          <Grid2 size={{ xs: 12, lg: 6 }}>
            <ThemePreview themeConfig={themeConfig} colourMode="dark" />
          </Grid2>
        </Grid2>
      </CardContent>
    </CardActionArea>
  </StyledCard>
)
