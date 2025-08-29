import { memo } from "react"

import { CheckCircle, Info, Warning, Error, Settings } from "@mui/icons-material"
import { Box, Button, Paper, Stack, styled, SupportedColorScheme, ThemeProvider, Typography } from "@mui/material"

import { SPACING } from "../../../../common/spacing"
import { JobStateChip } from "../../../../components/JobStateChip"
import { JobState } from "../../../../models/lookoutModels"
import { createLookoutTheme, DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS, LookoutThemeConfigOptions } from "../../../../theme"

const ThemePreviewContainer = styled("div")(({ theme }) => ({
  display: "flex",
  height: 260,
  width: 360,
  boxShadow: theme.shadows[2],
}))

const Screen = styled(Paper)(({ theme }) => ({
  background: theme.palette.background.default,
  flexGrow: 1,
}))

const PreviewAppBar = styled(Stack)(({ theme }) => ({
  padding: theme.spacing(SPACING.xs),
  backgroundColor: theme.palette.appBar.main,
  color: theme.palette.appBar.contrastText,
  alignItems: "center",
}))

const PreviewBody = styled(Stack)(({ theme }) => ({
  padding: theme.spacing(SPACING.xs),
}))

const PreviewPaper = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(SPACING.xs),
}))

const PreviewJobStateChipsContainer = styled("div")(({ theme }) => ({
  gap: theme.spacing(SPACING.xs * 0.5),
  display: "flex",
  flexWrap: "wrap",
}))

export interface ThemePreviewProps {
  themeConfig: LookoutThemeConfigOptions
  colourMode: SupportedColorScheme
}

export const ThemePreview = memo(({ themeConfig, colourMode }: ThemePreviewProps) => (
  <ThemePreviewContainer>
    <ThemeProvider theme={createLookoutTheme(themeConfig, colourMode)} storageManager={null}>
      <Screen>
        <div>
          <PreviewAppBar direction="row" spacing={SPACING.xs}>
            <Typography variant="h6" component="div" fontSize="0.7rem">
              Armada Lookout
            </Typography>
            <Box flex={1} />
            <Settings fontSize="inherit" />
          </PreviewAppBar>
        </div>
        <PreviewBody spacing={SPACING.sm}>
          <PreviewPaper>
            <Typography component="p" variant="body1" fontSize="0.75rem">
              This is some primary text.
            </Typography>
            <Typography component="p" variant="body2" color="textSecondary" gutterBottom fontSize="0.7rem">
              This is some secondary text.
            </Typography>
            <Typography component="div" variant="overline" fontSize="0.75rem">
              Overline text
            </Typography>
            <PreviewJobStateChipsContainer>
              {Object.values(JobState).map((jobState) => (
                <JobStateChip key={jobState} state={jobState} hideLabel />
              ))}
            </PreviewJobStateChipsContainer>
          </PreviewPaper>
          <Stack direction="row" spacing={SPACING.sm}>
            <div>
              <Button component="div" variant="contained" color="primary" size="small">
                Okay
              </Button>
            </div>
            <div>
              <Button component="div" variant="outlined" color="secondary" size="small">
                Cancel
              </Button>
            </div>
          </Stack>
          <Stack direction="row" spacing={SPACING.sm}>
            <div>
              <CheckCircle fontSize="small" color="success" />
            </div>
            <div>
              <Info fontSize="small" color="info" />
            </div>
            <div>
              <Warning fontSize="small" color="warning" />
            </div>
            <div>
              <Error fontSize="small" color="error" />
            </div>
          </Stack>
          <PreviewPaper>
            <code
              style={{
                fontFamily: themeConfig.monospaceFontFamily ?? DEFAULT_LOOKOUT_THEME_CONFIG_OPTIONS.monospaceFontFamily,
              }}
            >
              thisIsSomeCode
            </code>
          </PreviewPaper>
        </PreviewBody>
      </Screen>
    </ThemeProvider>
  </ThemePreviewContainer>
))
