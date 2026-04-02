import { useEffect, useRef, useState } from "react"

/**
 * useSessionTimer tracks elapsed seconds while active is true.
 * Resets to 0 when active becomes false.
 */
export function useSessionTimer(active: boolean): number {
  const [seconds, setSeconds] = useState(0)
  const startRef = useRef<number | null>(null)

  useEffect(() => {
    if (!active) {
      setSeconds(0)
      startRef.current = null
      return
    }

    startRef.current = Date.now()
    setSeconds(0)

    const interval = setInterval(() => {
      if (startRef.current !== null) {
        setSeconds(Math.floor((Date.now() - startRef.current) / 1000))
      }
    }, 1000)

    return () => clearInterval(interval)
  }, [active])

  return seconds
}

export function formatElapsed(seconds: number): string {
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = seconds % 60
  if (h > 0) {
    return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
  }
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
}
