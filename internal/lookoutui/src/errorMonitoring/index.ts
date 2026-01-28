import { getConfig } from "../config"

import { sentryReactErrorHandlers } from "./sentry"
import type { ReactErrorHandlers } from "./types"
import "./instrument"

const { sentry } = getConfig().errorMonitoring

const handlers: ReactErrorHandlers[] = []

if (sentry) {
  handlers.push(sentryReactErrorHandlers())
}

export const reactErrorHandlers: ReactErrorHandlers = {
  onCaughtError: (error, errorInfo) => {
    for (const { onCaughtError } of handlers) {
      onCaughtError?.(error, errorInfo)
    }
  },
  onUncaughtError: (error, errorInfo) => {
    for (const { onUncaughtError } of handlers) {
      onUncaughtError?.(error, errorInfo)
    }
  },
  onRecoverableError: (error, errorInfo) => {
    for (const { onRecoverableError } of handlers) {
      onRecoverableError?.(error, errorInfo)
    }
  },
}
