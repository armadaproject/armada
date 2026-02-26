import { useEffect, useRef } from "react"

import { AnalyticsScriptConfig } from "../config"
import { useUserManager } from "../oidcAuth"

export interface AnalyticsUserIdentifierProps {
  analyticsConfig: AnalyticsScriptConfig | undefined
}

/**
 * Analytics providers may not load immediately so add retry mechanism
 */
async function waitForAnalyticsProvider(provider: string): Promise<any | undefined> {
  const maxRetries = 20
  const retryDelay = 100
  for (let i = 0; i < maxRetries; i++) {
    if (typeof window !== "undefined" && provider in window) {
      const analytics = (window as any)[provider]
      if (typeof analytics === "object" && typeof analytics.identify === "function") {
        return analytics
      }
    }
    await new Promise((resolve) => setTimeout(resolve, retryDelay))
  }
  return undefined
}

/**
 * Component that identifies the authenticated user to the analytics provider.
 */
export const AnalyticsUserIdentifier = ({ analyticsConfig }: AnalyticsUserIdentifierProps) => {
  const userManager = useUserManager()
  const hasIdentified = useRef(false)

  useEffect(() => {
    if (hasIdentified.current || !analyticsConfig?.userIdentify || !userManager) {
      return
    }

    const identifyUser = async () => {
      const user = await userManager.getUser()
      if (!user) {
        return
      }

      const { provider, identifyParam } = analyticsConfig.userIdentify!
      const userId = user.profile.sub
      const analytics = await waitForAnalyticsProvider(provider)

      // identify functions either expect a string userId or an object with specified property - check config to determine which format to use
      if (analytics) {
        if (identifyParam) {
          analytics.identify({ [identifyParam]: userId })
        } else {
          analytics.identify(userId)
        }
        hasIdentified.current = true
      }
    }

    identifyUser()
  }, [analyticsConfig, userManager])

  return null
}
