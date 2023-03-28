import React from "react"

import { ThemeProvider as ThemeProviderV4, createTheme as createThemeV4, StylesProvider } from "@material-ui/core"
import { createGenerateClassName } from "@material-ui/core/styles"
import { ThemeProvider as ThemeProviderV5, createTheme as createThemeV5 } from "@mui/material/styles"
import { JobsTableContainer } from "containers/lookoutV2/JobsTableContainer"
import { SnackbarProvider } from "notistack"
import { Route, BrowserRouter as Router, Routes } from "react-router-dom"
import { IGetJobsService } from "services/lookoutV2/GetJobsService"
import { IGroupJobsService } from "services/lookoutV2/GroupJobsService"
import { JobsTablePreferencesService } from "services/lookoutV2/JobsTablePreferencesService"
import { UpdateJobsService } from "services/lookoutV2/UpdateJobsService"

import NavBar from "./components/NavBar"
import JobSetsContainer from "./containers/JobSetsContainer"
import JobsContainer from "./containers/JobsContainer"
import OverviewContainer from "./containers/OverviewContainer"
import { JobService } from "./services/JobService"
import LogService from "./services/LogService"
import { IGetJobSpecService } from "./services/lookoutV2/GetJobSpecService"
import { IGetRunErrorService } from "./services/lookoutV2/GetRunErrorService"

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
  jobService: JobService
  v2JobsTablePrefsService: JobsTablePreferencesService
  v2GetJobsService: IGetJobsService
  v2GroupJobsService: IGroupJobsService
  v2RunErrorService: IGetRunErrorService
  v2JobSpecService: IGetJobSpecService
  v2UpdateJobsService: UpdateJobsService
  logService: LogService
  overviewAutoRefreshMs: number
  jobSetsAutoRefreshMs: number
  jobsAutoRefreshMs: number
  debugEnabled: boolean
}
export function App(props: AppProps) {
  return (
    <StylesProvider generateClassName={generateClassName}>
      <ThemeProviderV4 theme={themeV4}>
        <ThemeProviderV5 theme={themeV5}>
          <SnackbarProvider
            anchorOrigin={{ horizontal: "right", vertical: "bottom" }}
            autoHideDuration={8000}
            maxSnack={3}
          >
            <Router>
              <div className="app-container">
                <NavBar />
                <div className="app-content">
                  <Routes>
                    <Route path="/">
                      <OverviewContainer {...props} />
                    </Route>
                    <Route path="/job-sets">
                      <JobSetsContainer {...props} />
                    </Route>
                    <Route path="/jobs">
                      <JobsContainer {...props} />
                    </Route>
                    <Route path="/v2">
                      <JobsTableContainer
                        jobsTablePreferencesService={props.v2JobsTablePrefsService}
                        getJobsService={props.v2GetJobsService}
                        groupJobsService={props.v2GroupJobsService}
                        updateJobsService={props.v2UpdateJobsService}
                        runErrorService={props.v2RunErrorService}
                        jobSpecService={props.v2JobSpecService}
                        debug={props.debugEnabled}
                      />
                    </Route>
                  </Routes>
                </div>
              </div>
            </Router>
          </SnackbarProvider>
        </ThemeProviderV5>
      </ThemeProviderV4>
    </StylesProvider>
  )
}
