import React, { CSSProperties } from 'react';
import { AppBar, Tabs, Tab, Toolbar, Typography } from '@material-ui/core'
import { BrowserRouter as Router, Link, Switch, Route } from 'react-router-dom'
import { Overview } from './Overview'

import './App.css';
import { JobService } from './services/jobs';
import { JobTableContainer } from "./containers/JobTableContainer";

const wrapperStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  height: "100%",
}

const contentStyle = {
  height: "100%",
  display: "flex"
}

export function App(services: { jobService: JobService; }) {
  const [value, setValue] = React.useState(0);

  const handleChange = (event: any, newValue: any) => {
    setValue(newValue);
  };

  return (
    <Router>
      <div style={wrapperStyle}>
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6">
              Armada Lookout
            </Typography>
          </Toolbar>
          <Tabs value={value} onChange={handleChange}>
            <Tab label="Overview" component={Link} to="/"/>
            <Tab label="Jobs" component={Link} to="/jobs"/>
          </Tabs>
        </AppBar>
        <div style={contentStyle}>
          <Switch>
            <Route path="/jobs">
              <JobTableContainer jobService={services.jobService}/>
            </Route>
            <Route path="/">
              <Overview jobService={services.jobService}/>
            </Route>
          </Switch>
        </div>
      </div>
    </Router>
  );
}
