import { AnalyticsScriptConfig } from "config"

/**
 * Builds analytics attributes based on the configured analytics provider
 * @param eventName The event name to track
 * @param analyticsConfig The analytics configuration
 * @param eventData Optional event data to include with the analytics event
 * @returns An object containing the appropriate data attributes or className for the provider
 */
export function buildAnalyticsAttributes(
  eventName: string,
  analyticsConfig: AnalyticsScriptConfig,
  eventData?: Record<string, string>,
): Record<string, string> {
  const method = analyticsConfig.method || "attribute"

  if (method === "class") {
    // Plausible-style: use CSS classes
    const classes: string[] = []

    if (analyticsConfig.eventAttribute) {
      // Replace spaces with + for Plausible class format
      const eventValue = eventName.replace(/\s/g, "+")
      classes.push(`${analyticsConfig.eventAttribute}=${eventValue}`)
    }

    if (analyticsConfig.dataAttribute && eventData) {
      Object.entries(eventData).forEach(([key, value]) => {
        // Replace spaces with + for Plausible class format
        const propValue = value.replace(/\s/g, "+")
        classes.push(`${analyticsConfig.dataAttribute}-${key.toLowerCase()}=${propValue}`)
      })
    }

    return { className: classes.join(" ") }
  } else {
    // Umami-style: use HTML attributes
    const analyticsAttributes: Record<string, string> = {}
    if (analyticsConfig.eventAttribute) {
      analyticsAttributes[analyticsConfig.eventAttribute] = eventName
    }
    if (analyticsConfig.dataAttribute && eventData) {
      Object.entries(eventData).forEach(([key, value]) => {
        analyticsAttributes[`${analyticsConfig.dataAttribute}-${key.toLowerCase()}`] = value
      })
    }

    return analyticsAttributes
  }
}
