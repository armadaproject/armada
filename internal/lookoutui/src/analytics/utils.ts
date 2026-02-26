import { AnalyticsScriptConfig } from "config"
import { User } from "oidc-client-ts"

/**
 * Identifies a user to the configured analytics provider
 * @param user The OIDC user object containing profile information
 * @param analyticsConfig The analytics configuration
 */
export function identifyUser(user: User, analyticsConfig: AnalyticsScriptConfig): void {
  if (!analyticsConfig.userIdentify || !user?.profile) {
    return
  }

  const { provider, identifyParam } = analyticsConfig.userIdentify
  const userId = user.profile.sub

  // Check if analytics provider is available.
  if (typeof window !== "undefined" && provider in window) {
    const analytics = (window as any)[provider]
    if (typeof analytics === "object" && typeof analytics.identify === "function") {
      if (identifyParam) {
        // Object format with specified key
        analytics.identify({ [identifyParam]: userId })
      } else {
        // String format - pass userId directly
        analytics.identify(userId)
      }
    }
  }
}

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
