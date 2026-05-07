import { Refresh } from "@mui/icons-material"
import { CircularProgress, IconButton, Typography } from "@mui/material"
import { useEffect, useRef, useState } from "react"

import "./RefreshButton.css"

type RefreshButtonProps = {
  isLoading: boolean
  onClick: () => void
}

function getRelativeTime(date: Date): string {
  const minutes = Math.floor((Date.now() - date.getTime()) / 60000)
  if (minutes < 1) return "just now"
  if (minutes < 60) return `${minutes} minute${minutes === 1 ? "" : "s"} ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours} hour${hours === 1 ? "" : "s"} ago`
  const days = Math.floor(hours / 24)
  return `${days} day${days === 1 ? "" : "s"} ago`
}

export default function RefreshButton(props: RefreshButtonProps) {
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [relativeTime, setRelativeTime] = useState("")
  const prevIsLoading = useRef(props.isLoading)

  useEffect(() => {
    if (prevIsLoading.current && !props.isLoading) {
      const now = new Date()
      setLastUpdated(now)
      setRelativeTime(getRelativeTime(now))
    }
    prevIsLoading.current = props.isLoading
  }, [props.isLoading])

  useEffect(() => {
    if (!lastUpdated) return
    setRelativeTime(getRelativeTime(lastUpdated))
    const interval = setInterval(() => setRelativeTime(getRelativeTime(lastUpdated)), 60000)
    return () => clearInterval(interval)
  }, [lastUpdated])

  return (
    <div className="refresh">
      {lastUpdated && (
        <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>
          Last updated {relativeTime}
        </Typography>
      )}
      {props.isLoading ? (
        <CircularProgress size={20} />
      ) : (
        <IconButton title={"Refresh"} onClick={props.onClick} color={"primary"} size="small">
          <Refresh />
        </IconButton>
      )}
    </div>
  )
}
