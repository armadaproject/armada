import { forwardRef } from "react"

import { Button, ButtonProps } from "@mui/material"

import { buildTrackingAttributes } from "../../common/trackingAttributes"
import { getConfig } from "../../config"

interface TrackingButtonProps extends ButtonProps {
  /**
   * The event name to track when the button is clicked
   */
  eventName: string
  /**
   * Optional event data to include with the tracking event
   */
  eventData?: Record<string, string>
}

/**
 * A button component that automatically adds the correct tracking attributes
 * based on the configured analytics provider.
 *
 * Supports:
 * - Attribute-based tracking (e.g., Umami): adds HTML attributes like data-umami-event="Event Name"
 * - Class-based tracking (e.g., Plausible): adds CSS classes like plausible-event-name=Event+Name
 *
 * Usage:
 * ```tsx
 * <TrackingButton eventName="Cancel Job" eventData={{ jobId: job.id }}>
 *   Cancel
 * </TrackingButton>
 * ```
 */
export const TrackingButton = forwardRef<HTMLButtonElement, TrackingButtonProps>(
  ({ eventName, eventData, ...buttonProps }, ref) => {
    const config = getConfig()
    const trackingConfig = config.trackingScript
    let trackingAttributes: Record<string, string> = {}

    if (trackingConfig && trackingConfig.trackedEvents?.includes(eventName)) {
      trackingAttributes = buildTrackingAttributes(eventName, trackingConfig, eventData)
    }

    return <Button ref={ref} {...buttonProps} {...trackingAttributes} />
  },
)

TrackingButton.displayName = "TrackingButton"
