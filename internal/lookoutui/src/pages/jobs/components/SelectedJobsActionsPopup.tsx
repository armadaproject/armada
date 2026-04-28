import { useCallback, useEffect, useRef, useState } from "react"

import { DragIndicator } from "@mui/icons-material"
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

  // null = use default CSS bottom/right anchor; once dragged, switch to top/left
  const [position, setPosition] = useState<{ x: number; y: number } | null>(null)
  const dragOffset = useRef<{ x: number; y: number } | null>(null)
  const isDragging = useRef(false)
  const prevCountRef = useRef(count)

  useEffect(() => {
    if (prevCountRef.current === 0 && count > 0) {
      setPosition(null)
    }
    prevCountRef.current = count
  }, [count])

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    isDragging.current = true
    dragOffset.current = {
      x: e.clientX - (position?.x ?? window.innerWidth - 244),
      y: e.clientY - (position?.y ?? window.innerHeight - 180),
    }
  }, [position])

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!isDragging.current || !dragOffset.current) return
      setPosition({
        x: e.clientX - dragOffset.current.x,
        y: e.clientY - dragOffset.current.y,
      })
    }
    const onMouseUp = () => {
      isDragging.current = false
    }
    window.addEventListener("mousemove", onMouseMove)
    window.addEventListener("mouseup", onMouseUp)
    return () => {
      window.removeEventListener("mousemove", onMouseMove)
      window.removeEventListener("mouseup", onMouseUp)
    }
  }, [])

  if (count === 0) return null

  const positionSx =
    position !== null
      ? { top: position.y, left: position.x }
      : { bottom: 24, right: 24 }

  return (
    <Paper
      elevation={6}
      sx={{
        position: "fixed",
        ...positionSx,
        zIndex: 1300,
        p: 2,
        display: "flex",
        flexDirection: "column",
        gap: 1.5,
        minWidth: 220,
        userSelect: "none",
      }}
    >
      <Box
        onMouseDown={onMouseDown}
        sx={{ display: "flex", alignItems: "center", gap: 0.5, cursor: "grab", "&:active": { cursor: "grabbing" } }}
      >
        <DragIndicator fontSize="small" sx={{ color: "text.secondary" }} />
        <Typography variant="subtitle2">
          {count} job{count !== 1 ? "s" : ""} selected
        </Typography>
      </Box>
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
