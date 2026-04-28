import { Box, Button, Paper, Typography } from "@mui/material"

import { JobFiltersWithExcludes } from "../../../models/lookoutModels"

interface SelectedJobsActionsPopupProps {
  selectedItemFilters: JobFiltersWithExcludes[]
  onCancel: () => void
  onReprioritize: () => void
  onPreempt: () => void
}

export const SelectedJobsActionsPopup = ({
  selectedItemFilters,
  onCancel,
  onReprioritize,
  onPreempt,
}: SelectedJobsActionsPopupProps) => {
  const count = selectedItemFilters.length
  if (count === 0) return null

  return (
    <Paper
      elevation={6}
      sx={{
        position: "fixed",
        bottom: 24,
        right: 24,
        zIndex: 1300,
        p: 2,
        display: "flex",
        flexDirection: "column",
        gap: 1.5,
        minWidth: 220,
      }}
    >
      <Typography variant="subtitle2">
        {count} job{count !== 1 ? "s" : ""} selected
      </Typography>
      <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
        <Button variant="contained" size="small" onClick={onCancel} fullWidth>
          Cancel selected
        </Button>
        <Button variant="contained" size="small" onClick={onReprioritize} fullWidth>
          Reprioritize selected
        </Button>
        <Button variant="contained" size="small" onClick={onPreempt} fullWidth>
          Preempt selected
        </Button>
      </Box>
    </Paper>
  )
}
