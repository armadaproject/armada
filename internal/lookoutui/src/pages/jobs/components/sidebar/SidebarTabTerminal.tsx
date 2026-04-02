import { useCallback, useMemo, useRef, useState } from "react"

import "@xterm/xterm/css/xterm.css"

import { Download } from "@mui/icons-material"
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Tooltip,
  Typography,
} from "@mui/material"

import { getContainers } from "../../../../common/containers"
import { Job } from "../../../../models/lookoutModels"
import { useGetJobSpec } from "../../../../services/lookout/useGetJobSpec"
import { useExecTerminal } from "../../../../hooks/useExecTerminal"
import { formatElapsed, useSessionTimer } from "../../../../hooks/useSessionTimer"

interface SidebarTabTerminalProps {
  job: Job
}

export const SidebarTabTerminal = ({ job }: SidebarTabTerminalProps) => {
  const [enabled, setEnabled] = useState(false)
  const [selectedContainer, setSelectedContainer] = useState("")
  const containerRef = useRef<HTMLDivElement | null>(null)

  // Fetch job spec to get container names and namespace
  const getJobSpecResult = useGetJobSpec(job.jobId, Boolean(job.jobId))
  const containers = useMemo(
    () => (getJobSpecResult.status === "success" ? getContainers(getJobSpecResult.data) : []),
    [getJobSpecResult.status, getJobSpecResult.data],
  )
  const namespace = useMemo(
    () => (getJobSpecResult.status === "success" ? (getJobSpecResult.data?.namespace ?? "") : ""),
    [getJobSpecResult.status, getJobSpecResult.data],
  )

  // Pick the container to exec into: explicit selection, or first available, or empty
  const activeContainer = selectedContainer || containers[0] || ""

  // Cluster from the most recent run
  const cluster = useMemo(() => {
    if (job.runs.length === 0) return ""
    return job.runs[job.runs.length - 1].cluster
  }, [job])

  const { status, exitCode, error, controls } = useExecTerminal(job.jobId, activeContainer, containerRef, enabled)

  const elapsedSeconds = useSessionTimer(status === "connected")

  const handleConnect = useCallback(() => {
    setEnabled(true)
  }, [])

  const handleReconnect = useCallback(() => {
    // Reset then re-enable so the hook effect re-runs
    setEnabled(false)
    setTimeout(() => setEnabled(true), 0)
  }, [])

  const isIdle = status === "idle"
  const isConnecting = status === "connecting"
  const isConnected = status === "connected"
  const isDisconnected = status === "disconnected"
  // Terminal should remain visible once it has been opened
  const terminalVisible = isConnected || isDisconnected

  return (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100%", gap: 1, p: 1 }}>
      {/* Pre-launch card: shown when idle */}
      {isIdle && (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            gap: 1.5,
            p: 1.5,
            border: "1px solid",
            borderColor: "divider",
            borderRadius: 1,
          }}
        >
          <Typography variant="subtitle2">Launch Terminal</Typography>
          {cluster && (
            <Typography variant="body2" color="text.secondary">
              <strong>Cluster:</strong> {cluster}
            </Typography>
          )}
          {namespace && (
            <Typography variant="body2" color="text.secondary">
              <strong>Namespace:</strong> {namespace}
            </Typography>
          )}
          {containers.length > 1 && (
            <FormControl variant="standard" size="small">
              <InputLabel id="exec-container-label">Container</InputLabel>
              <Select
                labelId="exec-container-label"
                value={selectedContainer || containers[0] || ""}
                onChange={(e) => setSelectedContainer(e.target.value as string)}
                sx={{ minWidth: "20ch" }}
              >
                {containers.map((c) => (
                  <MenuItem key={c} value={c}>
                    {c}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          {containers.length === 1 && (
            <Typography variant="body2" color="text.secondary">
              <strong>Container:</strong> {containers[0]}
            </Typography>
          )}
          <Button
            variant="contained"
            onClick={handleConnect}
            sx={{ alignSelf: "flex-start" }}
            disabled={getJobSpecResult.status === "pending"}
          >
            Connect
          </Button>
        </Box>
      )}

      {/* Connecting spinner */}
      {isConnecting && (
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <CircularProgress size={20} />
          <Typography variant="body2">Connecting…</Typography>
        </Box>
      )}

      {/* xterm.js container — visible when connected or disconnected to preserve scrollback */}
      <Box
        ref={containerRef}
        sx={{
          flex: "1 0 0",
          display: terminalVisible ? "block" : "none",
          overflow: "hidden",
          backgroundColor: "#000",
          "& .xterm": { height: "100%" },
          "& .xterm-viewport": { overflowY: "hidden" },
          // Dim terminal slightly when disconnected to signal read-only state
          opacity: isDisconnected ? 0.85 : 1,
        }}
      />

      {/* Status bar: shown while connected or disconnected */}
      {terminalVisible && (
        <>
          <Divider />
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 2,
              px: 0.5,
              flexWrap: "wrap",
            }}
          >
            {activeContainer && (
              <Typography variant="caption" color="text.secondary">
                container: <strong>{activeContainer}</strong>
              </Typography>
            )}
            {cluster && (
              <Typography variant="caption" color="text.secondary">
                cluster: <strong>{cluster}</strong>
              </Typography>
            )}
            {namespace && (
              <Typography variant="caption" color="text.secondary">
                ns: <strong>{namespace}</strong>
              </Typography>
            )}
            {isConnected && (
              <Typography variant="caption" color="text.secondary" aria-live="off">
                {formatElapsed(elapsedSeconds)}
              </Typography>
            )}
            <Box sx={{ flexGrow: 1 }} />
            <Tooltip title="Download transcript">
              <IconButton size="small" onClick={controls.downloadTranscript} aria-label="download transcript">
                <Download fontSize="small" />
              </IconButton>
            </Tooltip>
          </Box>
        </>
      )}

      {/* Disconnected overlay: shown below the visible terminal */}
      {isDisconnected && (
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
