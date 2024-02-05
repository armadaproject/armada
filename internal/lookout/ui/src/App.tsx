import React, { useEffect } from "react"

import { ThemeProvider as ThemeProviderV4, createTheme as createThemeV4, StylesProvider } from "@material-ui/core"
import { createGenerateClassName } from "@material-ui/core/styles"
import { ThemeProvider as ThemeProviderV5, createTheme as createThemeV5 } from "@mui/material/styles"
import { JobsTableContainer } from "containers/lookoutV2/JobsTableContainer"
import { SnackbarProvider } from "notistack"
import { UserManager, WebStorageStateStore } from "oidc-client-ts"
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { UpdateJobSetsService } from "services/lookoutV2/UpdateJobSetsService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"
import { withRouter } from "utils"

import NavBar from "./components/NavBar"
import JobSetsContainer from "./containers/JobSetsContainer"
import { UserManagerContext, useUserManager } from "./oidc"
import { ICordonService } from "./services/lookoutV2/CordonService"
import { IGetJobSpecService } from "./services/lookoutV2/GetJobSpecService"
import { IGetRunErrorService } from "./services/lookoutV2/GetRunErrorService"
import { ILogService } from "./services/lookoutV2/LogService"
import { CommandSpec } from "./utils"
import { OidcConfig } from "./utils"

import "./App.css"

// Required for Mui V4 and V5 to be compatible with each other
// See https://mui.com/x/react-data-grid/migration-v4/#using-mui-core-v4-with-v5
const generateClassName = createGenerateClassName({
  // By enabling this option, if you have non-MUI elements (e.g. `<div />`)
  // using MUI classes (e.g. `.MuiButton`) they will lose styles.
  // Make sure to convert them to use `styled()` or `<Box />` first.
  disableGlobal: true,
  // Class names will receive this seed to avoid name collisions.
  seed: "mui-jss",
})

const theme = {
  palette: {
    primary: {
      main: "#00aae1",
      contrastText: "#fff",
    },
  },
  typography: {
    fontFamily: [
      "-apple-system",
      "BlinkMacSystemFont",
      "'Segoe UI'",
      "'Roboto'",
      "'Oxygen'",
      "'Ubuntu'",
      "'Cantarell'",
      "'Fira Sans'",
      "'Droid Sans'",
      "'Helvetica Neue'",
      "sans-serif",
    ].join(","),
  },
}

const themeV4 = createThemeV4(theme)
const themeV5 = createThemeV5(theme)

type AppProps = {
  customTitle: string
  oidcConfig?: OidcConfig
  v2GetJobsService: IGetJobsService
  v2GroupJobsService: IGroupJobsService
  v2RunErrorService: IGetRunErrorService
  v2JobSpecService: IGetJobSpecService
  v2LogService: ILogService
  v2UpdateJobsService: UpdateJobsService
  v2UpdateJobSetsService: UpdateJobSetsService
  v2CordonService: ICordonService
  jobSetsAutoRefreshMs: number | undefined
  jobsAutoRefreshMs: number | undefined
  debugEnabled: boolean
  commandSpecs: CommandSpec[]
}

function OidcCallback(): JSX.Element {
  const userManager = useUserManager()
  const [error, setError] = React.useState<string | undefined>()
  React.useEffect(() => {
    userManager &&
      userManager.signinPopupCallback().catch((e) => {
        setError(`${e}`)
        console.error(e)
      })
  }, [])
  if (error !== undefined) return <p>Something went wrong; more details are available in the console.</p>
  return <p>Authenticating...</p>
}

// Version 2 of the Lookout UI used to be hosted under /v2, so we try our best
// to redirect users to the new location while preserving the rest of the URL.
const V2Redirect = withRouter(({ router }) => <Navigate to={{ ...router.location, pathname: "/" }} />)

export function App(props: AppProps): JSX.Element {
  useEffect(() => {
    if (props.customTitle) {
      document.title = `${props.customTitle} - Armada Lookout`
    }
  }, [props.customTitle])

  const result = (
    <StylesProvider generateClassName={generateClassName}>
      <ThemeProviderV4 theme={themeV4}>
        <ThemeProviderV5 theme={themeV5}>
          <SnackbarProvider
            anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
            autoHideDuration={8000}
            maxSnack={3}
          >
            <BrowserRouter>
              <div className="app-container">
                <NavBar customTitle={props.customTitle} />
                <div className="app-content">
                  <Routes>
                    <Route
                      path="/"
                      element={
                        <JobsTableContainer
                          getJobsService={props.v2GetJobsService}
                          groupJobsService={props.v2GroupJobsService}
                          updateJobsService={props.v2UpdateJobsService}
                          runErrorService={props.v2RunErrorService}
                          jobSpecService={props.v2JobSpecService}
                          logService={props.v2LogService}
                          cordonService={props.v2CordonService}
                          debug={props.debugEnabled}
                          autoRefreshMs={props.jobsAutoRefreshMs}
                          commandSpecs={props.commandSpecs}
                        />
                      }
                    />
                    <Route path="/job-sets" element={<JobSetsContainer {...props} />} />
                    <Route path="/oidc" element={<OidcCallback />} />
                    <Route path="/v2" element={<V2Redirect />} />
                    <Route
                      path="*"
                      element={
                        // This wildcard route ensures that users who follow old
                        // links to /job-sets or /jobs see something other than
                        // a blank page.
                        <Navigate to="/" />
                      }
                    />
                  </Routes>
                </div>
              </div>
            </BrowserRouter>
          </SnackbarProvider>
        </ThemeProviderV5>
      </ThemeProviderV4>
    </StylesProvider>
  )

  if (props.oidcConfig === undefined) return result

  return (
    <UserManagerContext.Provider
      value={
        new UserManager({
          authority: props.oidcConfig.authority,
          client_id: props.oidcConfig.clientId,
          redirect_uri: `${window.location.origin}/oidc`,
          scope: props.oidcConfig.scope,
          userStore: new WebStorageStateStore({ store: window.localStorage }),
        })
      }
    >
      {result}
    </UserManagerContext.Provider>
  )
}
