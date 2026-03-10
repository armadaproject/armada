import { useEffect, useRef } from "react"

import { AnalyticsConfig } from "../config"
import { useUserManager } from "../oidcAuth"

export interface AnalyticsUserIdentifierProps {
  analyticsConfig: AnalyticsConfig | undefined
  providerReady: boolean
}

/**
 * Component that identifies the authenticated user to the analytics provider.
 */
export const AnalyticsUserIdentifier = ({ analyticsConfig, providerReady }: AnalyticsUserIdentifierProps) => {
  const userManager = useUserManager()
  const identifiedUserId = useRef<string | null>(null)

  useEffect(() => {
    if (!analyticsConfig || !analyticsConfig.userIdentify?.trackUsers || !userManager || !providerReady) {
      return
    }

    // Identify the user to the analytics provider. When implementing an analytics solution refer to src/analytics/README.md#user-identification to see how configuration options change the function call
    const identifyUser = async () => {
      const user = await userManager.getUser()
      if (!user) {
        return
      }
      const provider = analyticsConfig.provider
      const identifyParam = analyticsConfig.userIdentify!.identifyParam
      const userId = user.profile.sub

      if (identifiedUserId.current === userId) {
        return
      }

      if (typeof window !== "undefined" && provider in window) {
        const analyticsProvider = (window as any)[provider]
        if (typeof analyticsProvider === "object" && typeof analyticsProvider.identify === "function") {
          if (identifyParam) {
            analyticsProvider.identify({ [identifyParam]: userId })
          } else {
            analyticsProvider.identify(userId)
          }
          identifiedUserId.current = userId
        }
      }
    }

    identifyUser()
  }, [analyticsConfig, userManager, providerReady])

  return null
}
