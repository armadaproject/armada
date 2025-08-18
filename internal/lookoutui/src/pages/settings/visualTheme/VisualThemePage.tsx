import { Stack, ToggleButton, ToggleButtonGroup, Typography, useColorScheme } from "@mui/material"

import { SPACING } from "../../../common/spacing"

export const VisualThemePage = () => {
  const { mode, setMode } = useColorScheme()
  return (
    <Stack spacing={SPACING.md}>
      <div>
        <Typography variant="h4">Colour mode</Typography>
      </div>
      <div>
        <Typography>Select your preferred colour mode, or sync it with your system.</Typography>
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
    </Stack>
  )
}
