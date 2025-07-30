import { useEffect } from "react"

import { CssBaseline, styled, ThemeProvider } from "@mui/material"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { SnackbarProvider } from "notistack"
import { ErrorBoundary } from "react-error-boundary"
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom"

import { AlertInPageContainerErrorFallback } from "./components/AlertInPageContainerErrorFallback"
import { FullPageErrorFallback } from "./components/FullPageErrorFallback"
import { JobIdRedirect } from "./components/JobIdRedirect"
import NavBar from "./components/NavBar"
import JobSetsContainer from "./containers/JobSetsContainer"
import { JobsTableContainer } from "./containers/lookout/JobsTableContainer"
import { OidcAuthProvider } from "./oidcAuth"
import { JOB_REDIRECT, JOB_SETS, JOBS, V2_REDIRECT } from "./pathnames"
import { ApiClientsProvider } from "./services/apiClients"
import { Services, ServicesProvider } from "./services/context"
import { theme } from "./theme/theme"
import { CommandSpec, OidcConfig, withRouter } from "./utils"

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
  customTitle: string
  oidcConfig?: OidcConfig
  services: Services
  jobSetsAutoRefreshMs: number | undefined
  jobsAutoRefreshMs: number | undefined
  debugEnabled: boolean
  commandSpecs: CommandSpec[]
}

// Version 2 of the Lookout UI used to be hosted under /v2, so we try our best
// to redirect users to the new location while preserving the rest of the URL.
const V2Redirect = withRouter(({ router }) => <Navigate to={{ ...router.location, pathname: JOBS }} />)

export function App(props: AppProps) {
  useEffect(() => {
    if (props.customTitle) {
      document.title = `${props.customTitle} - Armada Lookout`
    }
  }, [props.customTitle])

  return (
    <ErrorBoundary FallbackComponent={FullPageErrorFallback}>
      <ThemeProvider theme={theme} defaultMode="light">
        <CssBaseline />
        <SnackbarProvider
          anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
          autoHideDuration={8000}
          maxSnack={3}
        >
          <QueryClientProvider client={queryClient}>
            <ErrorBoundary FallbackComponent={FullPageErrorFallback}>
              <OidcAuthProvider oidcConfig={props.oidcConfig}>
                <ApiClientsProvider>
                  <BrowserRouter>
                    <ServicesProvider services={props.services}>
                      <AppContainer>
                        <NavBar customTitle={props.customTitle} />
                        <AppContent>
                          <Routes>
                            <Route
                              path={JOBS}
                              element={
                                <ErrorBoundary FallbackComponent={AlertInPageContainerErrorFallback}>
                                  <JobsTableContainer
                                    getJobsService={props.services.v2GetJobsService}
                                    groupJobsService={props.services.v2GroupJobsService}
                                    updateJobsService={props.services.v2UpdateJobsService}
                                    debug={props.debugEnabled}
                                    autoRefreshMs={props.jobsAutoRefreshMs}
                                    commandSpecs={props.commandSpecs}
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
                                  <JobSetsContainer
                                    v2GroupJobsService={props.services.v2GroupJobsService}
                                    v2UpdateJobSetsService={props.services.v2UpdateJobSetsService}
                                    jobSetsAutoRefreshMs={props.jobSetsAutoRefreshMs}
                                  />
                                </ErrorBoundary>
                              }
                            />
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
        </SnackbarProvider>
      </ThemeProvider>
    </ErrorBoundary>
  )
}
