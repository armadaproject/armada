import React from "react"

import { ThemeProvider, createMuiTheme } from "@material-ui/core"
import { Route, BrowserRouter as Router, Switch } from "react-router-dom"

import NavBar from "./components/NavBar"
import JobSetsContainer from "./containers/JobSetsContainer"
import JobsContainer from "./containers/JobsContainer"
import OverviewContainer from "./containers/OverviewContainer"
import JobService from "./services/JobService"

import "./App.css"

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

export function App(services: { jobService: JobService }) {
  return (
    <ThemeProvider theme={theme}>
      <Router>
        <div className="app-container">
          <NavBar />
          <div className="app-content">
            <Switch>
              <Route exact path="/">
                <OverviewContainer jobService={services.jobService} />
              </Route>
              <Route exact path="/job-sets">
                <JobSetsContainer jobService={services.jobService} />
              </Route>
              <Route exact path="/jobs">
                <JobsContainer jobService={services.jobService} />
              </Route>
            </Switch>
          </div>
        </div>
      </Router>
    </ThemeProvider>
  )
}
