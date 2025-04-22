import { Close } from "@mui/icons-material"
import {
  Alert,
  AlertTitle,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControlLabel,
  FormGroup,
  FormHelperText,
  Grid2,
  Stack,
  styled,
  Switch,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
  useColorScheme,
} from "@mui/material"
import { ErrorBoundary } from "react-error-boundary"

import { SPACING } from "../styling/spacing"
import {
  useCodeSnippetsWrapLines,
  useFormatNumberLocale,
  useFormatNumberNotation,
  useFormatNumberShouldFormat,
  useFormatTimestampLocale,
  useFormatTimestampShouldFormat,
  useFormatTimestampTimeZone,
  useJobRunLogsShowTimestamps,
  useJobRunLogsWrapLines,
} from "../userSettings"
import { AlertErrorFallback } from "./AlertErrorFallback"
import { JobRunLogsTextSizeToggle } from "./JobRunLogsTextSizeToggle"
import { LocaleSelector } from "./LocaleSelector"
import { NumberNotationSelector } from "./NumberNotationSelector"
import { TimeZoneSelector } from "./TimeZoneSelector"
import { TIMESTAMP_FORMATS, timestampFormatDisplayNames } from "../common/formatTime"
import { useFormatNumberWithUserSettings } from "../hooks/formatNumberWithUserSettings"
import { useFormatIsoTimestampWithUserSettings } from "../hooks/formatTimeWithUserSettings"

const FORMAT_NUMBER_PREVIEW_VALUES = [0, 0.034, 7, 888, 65536, 11197253, 6412378912]

const DialogContentContainer = styled("div")(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  gap: theme.spacing(SPACING.md),
}))

const SettingsGroupHeading = styled(Typography)({
  fontSize: "1.1rem",
  fontWeight: 500,
})

export interface SettingsDialogProps {
  open: boolean
  onClose: () => void
}

export const SettingsDialog = ({ open, onClose }: SettingsDialogProps) => {
  const { mode, setMode } = useColorScheme()

  const [jobRunLogsShowTimestamps, setJobRunLogsShowTimestamps] = useJobRunLogsShowTimestamps()
  const [jobRunLogsWrapLines, setJobRunLogsWrapLines] = useJobRunLogsWrapLines()
  const [codeSnippetsWrapLines, setCodeSnippetsWrapLines] = useCodeSnippetsWrapLines()
  const [formatTimestampShouldFormat, setFormatTimestampShouldFormat] = useFormatTimestampShouldFormat()
  const [formatTimestampTimeZone, setFormatTimestampTimeZone] = useFormatTimestampTimeZone()
  const [formatTimestampLocale, setFormatTimestampLocale] = useFormatTimestampLocale()
  const [formatNumberShouldFormat, setFormatNumberShouldFormat] = useFormatNumberShouldFormat()
  const [formatNumberLocale, setFormatNumberLocale] = useFormatNumberLocale()
  const [formatNumberNotation, setFormatNumberNotation] = useFormatNumberNotation()

  const formatIsoTimestamp = useFormatIsoTimestampWithUserSettings()
  const formatNumber = useFormatNumberWithUserSettings()

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Settings</DialogTitle>
      <DialogContent>
        <DialogContentContainer>
          <ErrorBoundary FallbackComponent={AlertErrorFallback}>
            <div>
              <div>
                <Typography variant="overline">Colour mode</Typography>
              </div>
              <div>
                <ToggleButtonGroup
                  color="primary"
                  disabled={mode === undefined}
                  value={mode ?? "system"}
                  exclusive
                  aria-label="colour mode"
                  onChange={(_, colorMode) => setMode(colorMode)}
                  fullWidth
                >
                  <ToggleButton value="light">Light</ToggleButton>
                  <ToggleButton value="system">System</ToggleButton>
                  <ToggleButton value="dark">Dark</ToggleButton>
                </ToggleButtonGroup>
              </div>
            </div>
            <div>
              <Divider />
            </div>
            <div>
              <SettingsGroupHeading variant="h2">Timestamp display</SettingsGroupHeading>
            </div>
            <div>
              <FormGroup>
                <FormControlLabel
                  control={
                    <Switch
                      checked={formatTimestampShouldFormat}
                      onChange={({ target: { checked } }) => setFormatTimestampShouldFormat(checked)}
                    />
                  }
                  label="Format timestamps"
                />
                {!formatTimestampShouldFormat && (
                  <FormHelperText>Timestamps will be displayed in the ISO 8601 format.</FormHelperText>
                )}
              </FormGroup>
            </div>
            {formatTimestampShouldFormat && (
              <Grid2 container spacing={SPACING.sm}>
                <Grid2 size={{ xs: 12, sm: 6 }}>
                  <LocaleSelector
                    idPrefix="settings-dialog-timestamps"
                    label="Locale for timestamps"
                    value={formatTimestampLocale}
                    onChange={setFormatTimestampLocale}
                    fullWidth
                    size="small"
                  />
                </Grid2>
                <Grid2 size={{ xs: 12, sm: 6 }}>
                  <TimeZoneSelector
                    idPrefix="settings-dialog-timestamps"
                    label="Time zone location for timestamps"
                    value={formatTimestampTimeZone}
                    onChange={setFormatTimestampTimeZone}
                    fullWidth
                    size="small"
                  />
                </Grid2>
              </Grid2>
            )}
            <Alert icon={false} severity="info">
              {formatTimestampShouldFormat ? (
                <>
                  <AlertTitle>Examples</AlertTitle>
                  <Stack direction="row" spacing={SPACING.sm} flexWrap="wrap">
                    <ul>
                      {TIMESTAMP_FORMATS.map((format) => (
                        <li key={format}>
                          {timestampFormatDisplayNames[format]}: {formatIsoTimestamp(new Date().toISOString(), format)}
                        </li>
                      ))}
                    </ul>
                  </Stack>
                </>
              ) : (
                <>
                  <AlertTitle>Example</AlertTitle>
                  <Stack direction="row" spacing={SPACING.sm} flexWrap="wrap">
                    {formatIsoTimestamp(new Date().toISOString(), "full")}
                  </Stack>
                </>
              )}
            </Alert>
            <div>
              <Divider />
            </div>
            <div>
              <SettingsGroupHeading variant="h2">Number display</SettingsGroupHeading>
            </div>
            <div>
              <FormGroup>
                <FormControlLabel
                  control={
                    <Switch
                      checked={formatNumberShouldFormat}
                      onChange={({ target: { checked } }) => setFormatNumberShouldFormat(checked)}
                    />
                  }
                  label="Format numbers"
                />
              </FormGroup>
            </div>
            {formatNumberShouldFormat && (
              <Grid2 container spacing={SPACING.sm}>
                <Grid2 size={{ xs: 12, sm: 6 }}>
                  <LocaleSelector
                    idPrefix="settings-dialog-numbers"
                    label="Locale for numbers"
                    value={formatNumberLocale}
                    onChange={setFormatNumberLocale}
                    fullWidth
                    size="small"
                  />
                </Grid2>
                <Grid2 size={{ xs: 12, sm: 6 }}>
                  <NumberNotationSelector
                    idPrefix="settings-dialog-numbers"
                    label="Number notation"
                    value={formatNumberNotation}
                    onChange={setFormatNumberNotation}
                    fullWidth
                    size="small"
                  />
                </Grid2>
              </Grid2>
            )}
            <Alert icon={false} severity="info">
              <AlertTitle>Examples</AlertTitle>
              <Stack direction="row" spacing={SPACING.sm} flexWrap="wrap">
                {FORMAT_NUMBER_PREVIEW_VALUES.map((v) => (
                  <div key={v}>{formatNumber(v)}</div>
                ))}
              </Stack>
            </Alert>
            <div>
              <Divider />
            </div>
            <div>
              <SettingsGroupHeading variant="h2">Job run logs</SettingsGroupHeading>
            </div>
            <Grid2 container spacing={SPACING.sm}>
              <Grid2 size={{ xs: 12, sm: 6 }}>
                <FormGroup>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={jobRunLogsShowTimestamps}
                        onChange={({ target: { checked } }) => setJobRunLogsShowTimestamps(checked)}
                      />
                    }
                    label="Show timestamps"
                  />
                </FormGroup>
              </Grid2>
              <Grid2 size={{ xs: 12, sm: 6 }}>
                <FormGroup>
                  <FormControlLabel
                    control={
                      <Switch
                        checked={jobRunLogsWrapLines}
                        onChange={({ target: { checked } }) => setJobRunLogsWrapLines(checked)}
                      />
                    }
                    label="Wrap lines"
                  />
                </FormGroup>
              </Grid2>
            </Grid2>
            <div>
              <div>
                <Typography variant="overline">Text size</Typography>
              </div>
              <div>
                <JobRunLogsTextSizeToggle />
              </div>
            </div>
            <div></div>
            <Divider />
            <div>
              <SettingsGroupHeading>Code snippets</SettingsGroupHeading>
            </div>
            <div>
              <Typography component="p">These settings do not apply to job run logs.</Typography>
            </div>
            <div>
              <FormGroup>
                <FormControlLabel
                  control={
                    <Switch
                      checked={codeSnippetsWrapLines}
                      onChange={({ target: { checked } }) => setCodeSnippetsWrapLines(checked)}
                    />
                  }
                  label="Wrap lines"
                />
              </FormGroup>
            </div>
          </ErrorBoundary>
        </DialogContentContainer>
      </DialogContent>
      <DialogActions>
        <Button autoFocus onClick={onClose} variant="outlined" endIcon={<Close />}>
          Close
        </Button>
      </DialogActions>
    </Dialog>
  )
}
