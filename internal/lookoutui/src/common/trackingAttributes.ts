import { TrackingScriptConfig } from "config"

/**
 * Builds tracking attributes based on the configured analytics provider
 * @param eventName The event name to track
 * @param trackingConfig The tracking configuration
 * @param eventData Optional event data to include with the tracking event
 * @returns An object containing the appropriate data attributes or className for the provider
 */
export function buildTrackingAttributes(
  eventName: string,
  trackingConfig: TrackingScriptConfig,
  eventData?: Record<string, string>,
): Record<string, string> {
  const method = trackingConfig.method || "attribute"

  if (method === "class") {
    // Plausible-style: use CSS classes
    const classes: string[] = []

    if (trackingConfig.eventAttribute) {
      // Replace spaces with + for Plausible class format
      const eventValue = eventName.replace(/\s/g, "+")
      classes.push(`${trackingConfig.eventAttribute}=${eventValue}`)
    }

    if (trackingConfig.dataAttribute && eventData) {
      Object.entries(eventData).forEach(([key, value]) => {
        // Replace spaces with + for Plausible class format
        const propValue = value.replace(/\s/g, "+")
        classes.push(`${trackingConfig.dataAttribute}-${key}=${propValue}`)
      })
    }

    return { className: classes.join(" ") }
  } else {
    // Umami-style: use HTML attributes
    const trackingAttributes: Record<string, string> = {}
    if (trackingConfig.eventAttribute) {
      trackingAttributes[trackingConfig.eventAttribute] = eventName
    }
    if (trackingConfig.dataAttribute && eventData) {
      Object.entries(eventData).forEach(([key, value]) => {
        trackingAttributes[`${trackingConfig.dataAttribute}-${key}`] = value
      })
    }

    return trackingAttributes
  }
}
