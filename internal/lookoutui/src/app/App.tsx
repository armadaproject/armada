import { useEffect } from "react"

import { CssBaseline, styled } from "@mui/material"
import { LocalizationProvider } from "@mui/x-date-pickers"
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { SnackbarProvider } from "notistack"
import { ErrorBoundary } from "react-error-boundary"
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom"

import { dayJsLocales, getBrowserSupportedLocale } from "../common/locales"
import { withRouter } from "../common/utils"
import { AlertInPageContainerErrorFallback } from "../components/AlertInPageContainerErrorFallback"
import { FullPageErrorFallback } from "../components/FullPageErrorFallback"
import { getConfig } from "../config"
import { LookoutThemeProvider } from "../lookoutThemeState"
import { OidcAuthProvider } from "../oidcAuth"
import { JobSetsPage } from "../pages/jobSets/JobSetsPage"
import { JobsPage } from "../pages/jobs/JobsPage"
import { SettingsPage } from "../pages/settings/SettingsPage"
import { AccountPage } from "../pages/settings/account/AccountPage"
import { AppearancePage } from "../pages/settings/appearance/AppearancePage"
import { ValueDisplayPage } from "../pages/settings/valueDisplay/ValueDisplayPage"
import { VisualThemePage } from "../pages/settings/visualTheme/VisualThemePage"
import {
  JOB_REDIRECT,
  JOB_SETS,
  JOBS,
  SETTINGS,
  SETTINGS_ACCOUNT,
  SETTINGS_APPEARANCE,
  SETTINGS_VALUE_DISPLAY,
  SETTINGS_VISUAL_THEME,
  V2_REDIRECT,
} from "../pathnames"
import { ApiClientsProvider } from "../services/apiClients"
import { Services, ServicesProvider } from "../services/context"
import { useFormatTimestampLocale } from "../userSettings"

import { JobIdRedirect } from "./JobIdRedirect"
import { NavBar } from "./NavBar"

const AppContainer = styled("div")({
  display: "flex",
  flexDirection: "column",
  height: "100vh",
})

const AppContent = styled("div")({
  height: "100%",
  minHeight: 0,
  display: "flex",
  flexDirection: "column",
})

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 0, retry: false, refetchOnMount: "always" },
  },
})

type AppProps = {
  services: Services
}

// Version 2 of the Lookout UI used to be hosted under /v2, so we try our best
// to redirect users to the new location while preserving the rest of the URL.
const V2Redirect = withRouter(({ router }) => <Navigate to={{ ...router.location, pathname: JOBS }} />)

const config = getConfig()

export function App(props: AppProps) {
  useEffect(() => {
    if (config.customTitle) {
      document.title = `${config.customTitle} - Armada Lookout`
    }
  }, [])

  const [formatTimestampLocale] = useFormatTimestampLocale()
  const supportedLocale = formatTimestampLocale === "browser" ? getBrowserSupportedLocale() : formatTimestampLocale
  const [dayJsLocale] = dayJsLocales[supportedLocale]

  return (
    <ErrorBoundary FallbackComponent={FullPageErrorFallback}>
      <LookoutThemeProvider>
        <CssBaseline />
        <SnackbarProvider
          anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
          autoHideDuration={8000}
          maxSnack={3}
        >
          <LocalizationProvider dateAdapter={AdapterDayjs} adapterLocale={dayJsLocale}>
            <QueryClientProvider client={queryClient}>
              <ErrorBoundary FallbackComponent={FullPageErrorFallback}>
                <OidcAuthProvider oidcConfig={config.oidcEnabled ? config.oidc : undefined}>
                  <ApiClientsProvider>
                    <BrowserRouter>
                      <ServicesProvider services={props.services}>
                        <AppContainer>
                          <NavBar customTitle={config.customTitle} />
                          <AppContent>
                            <Routes>
                              <Route
                                path={JOBS}
                                element={
                                  <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                    <JobsPage
                                      getJobsService={props.services.v2GetJobsService}
                                      groupJobsService={props.services.v2GroupJobsService}
                                      updateJobsService={props.services.v2UpdateJobsService}
                                      debug={config.debugEnabled}
                                      autoRefreshMs={config.jobsAutoRefreshMs}
                                      commandSpecs={config.commandSpecs}
                                    />
                                  </ErrorBoundary>
                                }
                              />
                              <Route
                                path={JOB_REDIRECT}
                                element={
                                  <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                    <JobIdRedirect />
                                  </ErrorBoundary>
                                }
                              />
                              <Route
                                path={JOB_SETS}
                                element={
                                  <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                    <JobSetsPage
                                      groupJobsService={props.services.v2GroupJobsService}
                                      updateJobSetsService={props.services.v2UpdateJobSetsService}
                                      autoRefreshMs={config.jobSetsAutoRefreshMs}
                                    />
                                  </ErrorBoundary>
                                }
                              />
                              <Route
                                path={SETTINGS}
                                element={
                                  <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                    <SettingsPage />
                                  </ErrorBoundary>
                                }
                              >
                                <Route
                                  path={SETTINGS_VISUAL_THEME}
                                  element={
                                    <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                      <VisualThemePage />
                                    </ErrorBoundary>
                                  }
                                />
                                <Route
                                  path={SETTINGS_VALUE_DISPLAY}
                                  element={
                                    <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                      <ValueDisplayPage />
                                    </ErrorBoundary>
                                  }
                                />
                                <Route
                                  path={SETTINGS_APPEARANCE}
                                  element={
                                    <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                      <AppearancePage />
                                    </ErrorBoundary>
                                  }
                                />
                                <Route
                                  path={SETTINGS_ACCOUNT}
                                  element={
                                    <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                      <AccountPage />
                                    </ErrorBoundary>
                                  }
                                />
                                <Route
                                  index
                                  element={
                                    <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                      <Navigate to={SETTINGS_VISUAL_THEME} replace />
                                    </ErrorBoundary>
                                  }
                                />
                              </Route>
                              <Route
                                path={V2_REDIRECT}
                                element={
                                  <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                    <V2Redirect />
                                  </ErrorBoundary>
                                }
                              />
                              <Route
                                path="*"
                                element={
                                  // This wildcard route ensures that users who follow old
                                  // links to /job-sets or /jobs see something other than
                                  // a blank page.
                                  <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                    <Navigate to={JOBS} />
                                  </ErrorBoundary>
                                }
                              />
                            </Routes>
                          </AppContent>
                        </AppContainer>
                      </ServicesProvider>
                    </BrowserRouter>
                  </ApiClientsProvider>
                </OidcAuthProvider>
              </ErrorBoundary>
            </QueryClientProvider>
          </LocalizationProvider>
        </SnackbarProvider>
      </LookoutThemeProvider>
    </ErrorBoundary>
  )
}
