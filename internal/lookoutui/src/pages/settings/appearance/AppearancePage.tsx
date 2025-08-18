import { Stack, Typography, Divider, FormControlLabel, FormGroup, Switch } from "@mui/material"

import { SPACING } from "../../../common/spacing"
import { JobRunLogsTextSizeToggle } from "../../../components/JobRunLogsTextSizeToggle"
import { useJobRunLogsShowTimestamps, useJobRunLogsWrapLines, useCodeSnippetsWrapLines } from "../../../userSettings"

export const AppearancePage = () => {
  const [jobRunLogsShowTimestamps, setJobRunLogsShowTimestamps] = useJobRunLogsShowTimestamps()
  const [jobRunLogsWrapLines, setJobRunLogsWrapLines] = useJobRunLogsWrapLines()
  const [codeSnippetsWrapLines, setCodeSnippetsWrapLines] = useCodeSnippetsWrapLines()
  return (
    <Stack spacing={SPACING.md}>
      <div>
        <Typography variant="h4">Code snippets</Typography>
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
      <div>
        <Typography>Choose your preferences for viewing code snippets (excluding job run logs).</Typography>
      </div>
      <div>
        <Divider variant="fullWidth" />
      </div>
      <div>
        <Typography variant="h4">Job run logs</Typography>
      </div>
      <div>
        <Typography>Choose your preferences for viewing the logs of job runs.</Typography>
      </div>
      <div>
        <Stack direction="row" spacing={SPACING.sm}>
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
        </Stack>
      </div>
      <div>
        <Typography variant="h5">Log text size</Typography>
      </div>
      <div>
        <JobRunLogsTextSizeToggle />
      </div>
    </Stack>
  )
}
