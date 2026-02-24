import { useEffect } from "react"

import { useUserManager } from "../oidcAuth"

export const useIdentifyUserForAnalytics = () => {
  const userManager = useUserManager()

  useEffect(() => {
    if (!userManager) {
      return
    }

    ;(async () => {
      const user = await userManager.getUser()
      if (!user || !user.profile) {
        return
      }

      // Check if umami is available
      if (typeof window !== "undefined" && "umami" in window && typeof window.umami === "object") {
        const umami = window.umami as { identify?: (id: string) => void }
        if (typeof umami.identify === "function") {
          umami.identify(user.profile.sub)
        }
      }
    })()
  }, [userManager])
}
