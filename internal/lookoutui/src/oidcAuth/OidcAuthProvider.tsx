import { ReactNode, useCallback, useEffect, useMemo, useState } from "react"

import { UserManager, WebStorageStateStore } from "oidc-client-ts"

import { ErrorPage } from "../components/ErrorPage"
import { OidcConfig } from "../utils"
import { OidcAuthContext, OidcAuthContextProps } from "./OidcAuthContext"
import { LoadingPage } from "../components/LoadingPage"

export const OIDC_REDIRECT_PATHNAME = "/oidc"

const ELLIPSIS = "\u2026"

const userManagerStore = new WebStorageStateStore({ store: window.localStorage })

export interface OidcAuthProviderProps {
  children: ReactNode
  oidcConfig: OidcConfig | undefined
}

export const OidcAuthProvider = ({ children, oidcConfig }: OidcAuthProviderProps) => {
  const [isLoading, setIsLoading] = useState(true)

  const userManager = useMemo<UserManager | undefined>(
    () =>
      oidcConfig
        ? new UserManager({
            authority: oidcConfig.authority,
            client_id: oidcConfig.clientId,
            redirect_uri: `${window.location.origin}${OIDC_REDIRECT_PATHNAME}`,
            scope: oidcConfig.scope,
            userStore: userManagerStore,
            loadUserInfo: true,
          })
        : undefined,
    [oidcConfig],
  )

  const [authError, setAuthError] = useState<any>(undefined)

  const isOidcRedirectPath = window.location.pathname === OIDC_REDIRECT_PATHNAME
  const authenticate = useCallback(async () => {
    setAuthError(undefined)
    setIsLoading(true)
    if (!userManager) {
      return
    }
    const user = await (isOidcRedirectPath ? userManager.signinRedirectCallback() : userManager.getUser())
    if (!user || user.expired) {
      return await userManager.signinRedirect({ state: window.location.href })
    }

    if (isOidcRedirectPath && typeof user.state === "string" && user.state) {
      const originalURL = new URL(user.state)
      // Preserve the current location's host, in case this has been changed by the redirect
      window.location.replace(`${originalURL.pathname}${originalURL.search}`)
    } else {
      setAuthError(undefined)
      setIsLoading(false)
    }
  }, [userManager, isOidcRedirectPath])

  const handlerAuthenticationError = useCallback((e: any) => {
    console.error(e)
    setAuthError(e)
    setIsLoading(false)
  }, [])

  useEffect(() => {
    if (!oidcConfig) {
      setIsLoading(false)
      return
    }

    authenticate().catch(handlerAuthenticationError)
  }, [authenticate, oidcConfig])

  const oidcAuthContextValue = useMemo<OidcAuthContextProps>(() => ({ userManager }), [userManager])

  if (isLoading) {
    return <LoadingPage loadingContextMessage={`Signing you in${ELLIPSIS}`} />
  }

  if (authError) {
    return (
      <ErrorPage
        error={authError}
        errorTitle="Sorry, there was an error signing you into Armada Lookout"
        retry={() => authenticate().catch(handlerAuthenticationError)}
        errorContextMessage="Please check the console for more details."
      />
    )
  }

  return <OidcAuthContext.Provider value={oidcAuthContextValue}>{children}</OidcAuthContext.Provider>
}
