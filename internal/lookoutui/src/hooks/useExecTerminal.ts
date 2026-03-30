import { RefObject, useEffect, useRef, useState } from "react"

import { FitAddon } from "@xterm/addon-fit"
import { Terminal } from "@xterm/xterm"

export type ExecStatus = "idle" | "connecting" | "connected" | "disconnected"

export interface ExecTerminalResult {
  status: ExecStatus
  exitCode: number | null
  error: string | null
}

/**
 * useExecTerminal opens a WebSocket exec session and renders an xterm.js terminal
 * into containerRef. Set enabled=true to start the session.
 */
export function useExecTerminal(
  jobId: string,
  containerRef: RefObject<HTMLDivElement | null>,
  enabled: boolean,
): ExecTerminalResult {
  const [status, setStatus] = useState<ExecStatus>("idle")
  const [exitCode, setExitCode] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Hold terminal/ws/addon refs so effect cleanup can dispose them
  const terminalRef = useRef<Terminal | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const fitAddonRef = useRef<FitAddon | null>(null)

  useEffect(() => {
    if (!enabled || !containerRef.current) {
      return
    }

    setStatus("connecting")
    setExitCode(null)
    setError(null)

    const terminal = new Terminal({ cursorBlink: true })
    const fitAddon = new FitAddon()
    terminal.loadAddon(fitAddon)
    terminal.open(containerRef.current)
    fitAddon.fit()

    terminalRef.current = terminal
    fitAddonRef.current = fitAddon

    // Build WebSocket URL from current location
    const proto = window.location.protocol === "https:" ? "wss" : "ws"
    const wsUrl = `${proto}://${window.location.host}/api/exec/ws?jobId=${encodeURIComponent(jobId)}`
    const ws = new WebSocket(wsUrl)
    ws.binaryType = "arraybuffer"
    wsRef.current = ws

    ws.onopen = () => {
      setStatus("connected")
      // Send initial resize
      sendResize(ws, terminal.cols, terminal.rows)
    }

    ws.onmessage = (event: MessageEvent) => {
      const data = new Uint8Array(event.data as ArrayBuffer)
      if (data.length === 0) return
      const msgType = data[0]
      const payload = data.slice(1)

      if (msgType === 0x00) {
        // stdout/stderr output
        terminal.write(payload)
      } else if (msgType === 0x01) {
        // exit code: 4-byte int32 BE
        if (payload.length >= 4) {
          const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength)
          const code = view.getInt32(0, false /* big-endian */)
          setExitCode(code)
        }
        setStatus("disconnected")
        ws.close()
      } else if (msgType === 0x02) {
        // error message
        const errMsg = new TextDecoder().decode(payload)
        setError(errMsg)
        setStatus("disconnected")
        ws.close()
      }
    }

    ws.onclose = () => {
      setStatus((prev) => (prev === "connected" || prev === "connecting" ? "disconnected" : prev))
    }

    ws.onerror = () => {
      setError("WebSocket connection error")
      setStatus("disconnected")
    }

    // Forward terminal input to server
    const onDataDispose = terminal.onData((data) => {
      if (ws.readyState !== WebSocket.OPEN) return
      const encoded = new TextEncoder().encode(data)
      const frame = new Uint8Array(1 + encoded.length)
      frame[0] = 0x00
      frame.set(encoded, 1)
      ws.send(frame)
    })

    // Forward resize events to server
    const onResizeDispose = terminal.onResize(({ cols, rows }) => {
      if (ws.readyState === WebSocket.OPEN) {
        sendResize(ws, cols, rows)
      }
    })

    // ResizeObserver: refit terminal when the container changes size
    const resizeObserver = new ResizeObserver(() => {
      fitAddon.fit()
    })
    resizeObserver.observe(containerRef.current)

    return () => {
      onDataDispose.dispose()
      onResizeDispose.dispose()
      resizeObserver.disconnect()
      ws.close()
      terminal.dispose()
      terminalRef.current = null
      wsRef.current = null
      fitAddonRef.current = null
    }
  }, [enabled, jobId]) // re-run when jobId changes or enabled toggles

  return { status, exitCode, error }
}

function sendResize(ws: WebSocket, cols: number, rows: number): void {
  const frame = new Uint8Array(5)
  frame[0] = 0x01
  const view = new DataView(frame.buffer)
  view.setUint16(1, cols, false /* big-endian */)
  view.setUint16(3, rows, false /* big-endian */)
  ws.send(frame)
}
