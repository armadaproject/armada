import { useCallback, useRef, useState } from "react"

import "@xterm/xterm/css/xterm.css"

import { Alert, Box, Button, CircularProgress, Typography } from "@mui/material"

import { Job } from "../../../../models/lookoutModels"
import { useExecTerminal } from "../../../../hooks/useExecTerminal"

interface SidebarTabTerminalProps {
  job: Job
}

export const SidebarTabTerminal = ({ job }: SidebarTabTerminalProps) => {
  const [enabled, setEnabled] = useState(false)
  const containerRef = useRef<HTMLDivElement | null>(null)

  const { status, exitCode, error } = useExecTerminal(job.jobId, containerRef, enabled)

  const handleLaunch = useCallback(() => {
    setEnabled(true)
  }, [])

  const handleReconnect = useCallback(() => {
    // Reset then re-enable so the hook effect re-runs
    setEnabled(false)
    setTimeout(() => setEnabled(true), 0)
  }, [])

  return (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100%", gap: 1, p: 1 }}>
      {/* Launch button: shown when idle */}
      {status === "idle" && (
        <Button variant="contained" onClick={handleLaunch} sx={{ alignSelf: "flex-start" }}>
          Launch Terminal
        </Button>
      )}

      {/* Connecting spinner */}
      {status === "connecting" && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <CircularProgress size={20} />
          <Typography variant="body2">Connecting…</Typography>
        </Box>
      )}

      {/* xterm.js container — always rendered so the ref is stable, hidden until connected */}
      <Box
        ref={containerRef}
        sx={{
          flex: "1 0 0",
          display: status === "connected" ? "block" : "none",
          overflow: "hidden",
          backgroundColor: "#000",
          "& .xterm": { height: "100%" },
          "& .xterm-viewport": { overflowY: "hidden" },
        }}
      />

      {/* Disconnected overlay with exit code / error and Reconnect button */}
      {status === "disconnected" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
          {error ? (
            <Alert severity="error">{error}</Alert>
          ) : (
            <Alert severity={exitCode === 0 ? "success" : "warning"}>
              Session ended — exit code {exitCode ?? "unknown"}
            </Alert>
          )}
          <Button variant="outlined" onClick={handleReconnect} sx={{ alignSelf: "flex-start" }}>
            Reconnect
          </Button>
        </Box>
      )}
    </Box>
  )
}
