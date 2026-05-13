import { getConfig } from "../config"

import { AnalyticsEventName } from "./types"

const config = getConfig()

/**
 * Send an analytics event to the configured provider.
 * Use  this helper only when the event isn't tied to a DOM click (e.g. dialog close)
 *
 * @param eventName - The event name to track
 * @param eventData - Optional key-value pairs to include with the event
 */
export const trackAnalyticsEvent = (eventName: AnalyticsEventName, eventData?: Record<string, string>) => {
  const analyticsConfig = config.analytics

  if (!analyticsConfig) {
    return
  }

  const provider = analyticsConfig.provider
  const trackFunction = analyticsConfig.customEventFunction ?? "track"

  if (typeof window === "undefined" || !(provider in window)) {
    return
  }

  const analyticsProvider = (window as any)[provider]
  const dataToSend = analyticsConfig.dataWrapper && eventData ? { [analyticsConfig.dataWrapper]: eventData } : eventData

  try {
    if (typeof analyticsProvider === "function") {
      analyticsProvider(eventName, dataToSend)
    } else if (typeof analyticsProvider === "object" && typeof analyticsProvider[trackFunction] === "function") {
      analyticsProvider[trackFunction](eventName, dataToSend)
    }
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error("Analytics provider error:", e)
  }
}
