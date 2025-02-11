import { Close } from "@mui/icons-material"
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
  useColorScheme,
} from "@mui/material"

export interface SettingsDialogProps {
  open: boolean
  onClose: () => void
}

export const SettingsDialog = ({ open, onClose }: SettingsDialogProps) => {
  const { mode, setMode } = useColorScheme()

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>Settings</DialogTitle>
      <DialogContent>
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
      </DialogContent>
      <DialogActions>
        <Button autoFocus onClick={onClose} variant="outlined" endIcon={<Close />}>
          Close
        </Button>
      </DialogActions>
    </Dialog>
  )
}
