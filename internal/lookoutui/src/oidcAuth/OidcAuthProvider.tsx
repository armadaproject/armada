import { ReactNode, useCallback, useEffect, useMemo, useState } from "react"

import { Alert, AlertTitle, Button, Container, LinearProgress, styled, Typography } from "@mui/material"
import { UserManager, WebStorageStateStore } from "oidc-client-ts"

import { SPACING } from "../styling/spacing"
import { OidcConfig } from "../utils"
import { OidcAuthContext, OidcAuthContextProps } from "./OidcAuthContext"

export const OIDC_REDIRECT_PATHNAME = "/oidc"

const ELLIPSIS = "\u2026"

const userManagerStore = new WebStorageStateStore({ store: window.localStorage })

const Wrapper = styled("main")(({ theme }) => ({
  minHeight: "100vh",
  backgroundColor: theme.palette.background.default,
  display: "flex",
  justifyContent: "center",
  alignItems: "center",
}))

const ContentContainer = styled(Container)(({ theme }) => ({
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  gap: theme.spacing(SPACING.xl),
}))

const ProgressContainer = styled("div")({
  width: "100%",
})

const StyledAlert = styled(Alert)({
  width: "100%",
})

const IconImg = styled("img")({
  maxHeight: 200,
})

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
      return await userManager.signinRedirect()
    }

    setAuthError(undefined)
    setIsLoading(false)
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
    return (
      <Wrapper>
        <ContentContainer maxWidth="md">
          <div>
            <IconImg src="/logo.svg" alt="Armada Lookout" />
          </div>
          <ProgressContainer>
            <LinearProgress />
          </ProgressContainer>
          <div>
            <Typography component="p" variant="h4" textAlign="center">
              Armada Lookout
            </Typography>
            <Typography component="p" variant="h6" color="text.secondary" textAlign="center">
              Signing you in{ELLIPSIS}
            </Typography>
          </div>
        </ContentContainer>
      </Wrapper>
    )
  }

  if (authError) {
    return (
      <Wrapper>
        <ContentContainer maxWidth="md">
          <div>
            <IconImg src="/logo.svg" alt="Armada Lookout" />
          </div>
          <StyledAlert
            severity="error"
            action={
              <Button color="inherit" size="small" onClick={() => authenticate().catch(handlerAuthenticationError)}>
                Retry
              </Button>
            }
          >
            <AlertTitle>Sorry, there was an error signing you into Armada Lookout</AlertTitle>
            <Typography component="p">{String(authError)}</Typography>
            <Typography component="p">Please check the console for more details.</Typography>
          </StyledAlert>
        </ContentContainer>
      </Wrapper>
    )
  }

  return <OidcAuthContext.Provider value={oidcAuthContextValue}>{children}</OidcAuthContext.Provider>
}
