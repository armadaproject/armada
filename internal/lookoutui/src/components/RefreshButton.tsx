import { useEffect, useRef, useState } from "react"

import { Refresh } from "@mui/icons-material"
import { CircularProgress, IconButton, Stack, Typography } from "@mui/material"

import { formatTimestampRelative } from "../common/formatTime"

type RefreshButtonProps = {
  isLoading: boolean
  onClick: () => void
}

export default function RefreshButton(props: RefreshButtonProps) {
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [, setTick] = useState(false)
  const wasLoading = useRef(false)

  useEffect(() => {
    if (wasLoading.current && !props.isLoading) {
      setLastUpdated(new Date())
    }
    wasLoading.current = props.isLoading
  }, [props.isLoading])

  useEffect(() => {
    if (!lastUpdated) {
      return
    }
    const id = setInterval(() => setTick((t) => !t), 60_000)
    return () => clearInterval(id)
  }, [lastUpdated])

  return (
    <Stack direction="row" alignItems="center">
      {lastUpdated && (
        <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>
          Last updated {formatTimestampRelative(lastUpdated, true)}
        </Typography>
      )}
      {props.isLoading ? (
        <CircularProgress size={20} />
      ) : (
        <IconButton title={"Refresh"} onClick={props.onClick} color={"primary"} size="small">
          <Refresh />
        </IconButton>
      )}
    </Stack>
  )
}
