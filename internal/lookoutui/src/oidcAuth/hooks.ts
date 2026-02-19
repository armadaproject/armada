import { useCallback, useContext, useEffect, useState } from "react"

import { UserManager } from "oidc-client-ts"

import { OidcAuthContext } from "./OidcAuthContext"
import { appendAuthorizationHeaders } from "./utils"

export const useUserManager = (): UserManager | undefined => useContext(OidcAuthContext)?.userManager

export const useUsername = (): string | null => {
  const userManager = useUserManager()
  const [username, setUsername] = useState<string | null>(null)
  useEffect(() => {
    if (!userManager) {
      return
    }

    ;(async () => {
      const user = await userManager.getUser()
      if (!user) {
        return
      }

      setUsername(user.profile.sub)
    })()
  }, [userManager])

  return username
}

/**
 * Hook that automatically identifies the user with Umami analytics when authenticated.
 * Call this hook in a top-level component to track the authenticated user across sessions.
 */
export const useIdentifyUserForTracking = () => {
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

export const useGetAccessToken = () => {
  const userManager = useUserManager()

  return useCallback(async () => {
    if (!userManager) {
      return undefined
    }

    const user = await userManager.getUser()
    if (!user || user.expired) {
      await userManager.signinRedirect({ state: window.location.href })
      return undefined
    }

    return user.access_token
  }, [userManager])
}

export const useAuthenticatedFetch = () => {
  const getAccessToken = useGetAccessToken()
  return useCallback<GlobalFetch["fetch"]>(
    (input, init) =>
      getAccessToken().then((accessToken) => {
        const headers = new Headers(init?.headers)
        if (accessToken) {
          appendAuthorizationHeaders(headers, accessToken)
        }
        return fetch(input, { ...init, headers })
      }),
    [getAccessToken],
  )
}
