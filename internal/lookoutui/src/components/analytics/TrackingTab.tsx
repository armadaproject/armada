import { forwardRef, MouseEvent, useCallback } from "react"

import { Tab, TabProps } from "@mui/material"

import { buildTrackingAttributes } from "../../common/trackingAttributes"
import { getConfig } from "../../config/config"

interface TrackingTabProps extends TabProps {
  /**
   * The event name to track when the tab is clicked
   */
  eventName: string
  /**
   * Optional event data to include with the tracking event
   */
  eventData?: Record<string, string>
}

/**
 * A tab component that automatically adds the correct tracking attributes
 * based on the configured analytics provider (Umami, Plausible, Google Analytics, etc.)
 *
 * Usage:
 * ```tsx
 * <TrackingTab
 *   label="Details"
 *   value="details"
 *   eventName="Sidebar Tab View"
 *   eventData={{ tab: "details" }}
 * />
 * ```
 */
export const TrackingTab = forwardRef<HTMLDivElement, TrackingTabProps>(
  ({ eventName, eventData, onClick, ...tabProps }, ref) => {
    const config = getConfig()
    const trackingConfig = config.trackingScript
    let trackingAttributes: Record<string, string> = {}

    if (trackingConfig && trackingConfig.trackedEvents?.includes(eventName)) {
      trackingAttributes = buildTrackingAttributes(eventName, trackingConfig, eventData)
    }

    const handleClick = useCallback(
      (event: MouseEvent<HTMLDivElement>) => {
        // Dispatch custom event for tracking scripts that need it
        if (trackingConfig && trackingConfig.trackedEvents?.includes(eventName)) {
          const customEvent = new CustomEvent("tracking", { detail: trackingAttributes })
          window.dispatchEvent(customEvent)
        }

        // Call original onClick if provided
        if (onClick) {
          onClick(event)
        }
      },
      [onClick, eventName, trackingAttributes, trackingConfig],
    )

    return <Tab ref={ref} {...tabProps} {...trackingAttributes} onClick={handleClick} />
  },
)

TrackingTab.displayName = "TrackingTab"
