import { useCallback, useContext, useEffect, useState } from "react"

import { UserManager } from "oidc-client-ts"

import { getConfig } from "../config"
import { mirrorLookoutApiRequest } from "../lookoutApiRequestMirror"

import { OidcAuthContext } from "./OidcAuthContext"
import { appendAuthorizationHeaders } from "./utils"

const config = getConfig()

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

      if (config.oidc?.displayNameClaim) {
        const displayName = user.profile[config.oidc.displayNameClaim]
        if (typeof displayName === "string") {
          setUsername(displayName)
          return
        }
        // eslint-disable-next-line no-console
        console.warn(
          `[useUsername] displayNameClaim "${config.oidc.displayNameClaim}" not found or not a string in OIDC profile; falling back to "sub"`,
        )
      }
      setUsername(user.profile.sub)
    })()
  }, [userManager])

  return username
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

// useAuthenticatedFetchInternal returns a fetch function that attaches the
// current access token, and additionally invokes onRequest (if given) with the
// final input and token-bearing init just before the request is sent. This is
// the shared core of useAuthenticatedFetch and useMirroredLookoutApiFetch.
const useAuthenticatedFetchInternal = (onRequest?: (input: RequestInfo | URL, init: RequestInit) => void) => {
  const getAccessToken = useGetAccessToken()
  return useCallback<GlobalFetch["fetch"]>(
    (input, init) =>
      getAccessToken().then((accessToken) => {
        const headers = new Headers(init?.headers)
        if (accessToken) {
          appendAuthorizationHeaders(headers, accessToken)
        }
        const authenticatedInit = { ...init, headers }
        onRequest?.(input, authenticatedInit)
        return fetch(input, authenticatedInit)
      }),
    [getAccessToken, onRequest],
  )
}

export const useAuthenticatedFetch = () => useAuthenticatedFetchInternal()

// useMirroredLookoutApiFetch behaves like useAuthenticatedFetch but also mirrors
// each Lookout API request to the configured mirror backend. Use it in place of
// useAuthenticatedFetch for calls to the Lookout server's own REST API, so that
// only Lookout API load is mirrored; requests to the Armada server or
// Binoculars should continue to use useAuthenticatedFetch directly.
export const useMirroredLookoutApiFetch = () => useAuthenticatedFetchInternal(mirrorLookoutApiRequest)
