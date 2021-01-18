import React from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom'
import { ThemeProvider, createMuiTheme } from "@material-ui/core";

import JobService from './services/JobService';
import OverviewContainer from './containers/OverviewContainer'
import JobSetsContainer from "./containers/JobSetsContainer";
import JobsContainer from "./containers/JobsContainer";
import NavBar from "./components/NavBar";

import './App.css';

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
    ].join(','),
  },
})

export function App(services: { jobService: JobService; }) {
  return (
    <ThemeProvider theme={theme}>
      <Router>
        <div className="container">
          <NavBar/>
          <div className="content">
            <Switch>
              <Route exact path="/">
                <OverviewContainer jobService={services.jobService}/>
              </Route>
              <Route exact path="/job-sets">
                <JobSetsContainer jobService={services.jobService}/>
              </Route>
              <Route exact path="/jobs">
                <JobsContainer jobService={services.jobService}/>
              </Route>
            </Switch>
          </div>
        </div>
      </Router>
    </ThemeProvider>
  );
}
