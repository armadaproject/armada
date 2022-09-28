import React from "react"

import { ThemeProvider, createMuiTheme } from "@material-ui/core"
import { Route, BrowserRouter as Router, Switch } from "react-router-dom"

import NavBar from "./components/NavBar"
import JobSetsContainer from "./containers/JobSetsContainer"
import JobsContainer from "./containers/JobsContainer"
import OverviewContainer from "./containers/OverviewContainer"
import { JobService } from "./services/JobService"
import LogService from "./services/LogService"

import "./App.css"

type AppProps = {
  jobService: JobService
  logService: LogService
  overviewAutoRefreshMs: number
  jobSetsAutoRefreshMs: number
  jobsAutoRefreshMs: number
}

const theme = createMuiTheme({
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
})

export function App(props: AppProps) {
  return (
    <ThemeProvider theme={theme}>
      <Router>
        <div className="app-container">
          <NavBar />
          <div className="app-content">
            <Switch>
              <Route exact path="/">
                <OverviewContainer {...props} />
              </Route>
              <Route exact path="/job-sets">
                <JobSetsContainer {...props} />
              </Route>
              <Route exact path="/jobs">
                <JobsContainer {...props} />
              </Route>
            </Switch>
          </div>
        </div>
      </Router>
    </ThemeProvider>
  )
}
