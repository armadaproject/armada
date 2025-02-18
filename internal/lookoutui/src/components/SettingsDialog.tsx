import { Close } from "@mui/icons-material"
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControlLabel,
  FormGroup,
  styled,
  Switch,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
  useColorScheme,
} from "@mui/material"

import { SPACING } from "../styling/spacing"
import { useCodeSnippetsWrapLines, useJobRunLogsShowTimestamps, useJobRunLogsWrapLines } from "../userSettings"
import { JobRunLogsTextSizeToggle } from "./JobRunLogsTextSizeToggle"

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

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>Settings</DialogTitle>
      <DialogContent>
        <DialogContentContainer>
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
            <SettingsGroupHeading variant="h2">Job run logs</SettingsGroupHeading>
          </div>
          <div>
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
          </div>
          <div>
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
          </div>
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
