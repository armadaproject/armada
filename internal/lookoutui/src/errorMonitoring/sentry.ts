import * as Sentry from "@sentry/react"

import { SentryConfig } from "../config"
import type { ReactErrorHandlers } from "./types"

export const initialiseSentry = ({ dsn, environment }: SentryConfig) => {
  Sentry.init({ dsn, environment })
}

export const sentryReactErrorHandlers = (): ReactErrorHandlers => ({
  onCaughtError: Sentry.reactErrorHandler(),
  onRecoverableError: Sentry.reactErrorHandler(),
  onUncaughtError: Sentry.reactErrorHandler(),
})
