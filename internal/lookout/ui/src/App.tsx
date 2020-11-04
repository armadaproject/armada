import React from 'react';
import { AppBar, Tabs, Tab, Toolbar, Typography } from '@material-ui/core'
import { BrowserRouter as Router, Link, Switch, Route } from 'react-router-dom'
import { Overview } from './Overview'

import './App.css';
import { JobService } from './services/jobs';

export function App(services: { jobService: JobService; }) {
  return (
    <Router>
        <AppBar position="static">
            <Toolbar>
                <Typography variant="h6">
                  Armada Lookout
                </Typography>
            </Toolbar>
            <Tabs>
                <Tab label="Overview" component={Link} to="/" />
                <Tab label="Jobs" component={Link} to="/jobs" />
            </Tabs>
        </AppBar>
        <div>
            
            <Switch>
                <Route path="/jobs">
                    Jobs
                </Route>
                <Route path="/">
                    <Overview jobService={services.jobService}/>
                </Route>
            </Switch>
        </div>
    </Router>

    // <div className="App">
    //   <header className="App-header">
    //     <img src={logo} className="App-logo" alt="logo" />
    //     <p>
    //       Edit <code>src/App.tsx</code> and save to reload.
    //     </p>
    //     <a
    //       className="App-link"
    //       href="https://reactjs.org"
    //       target="_blank"
    //       rel="noopener noreferrer"
    //     >
    //       Learn React
    //     </a>
    //   </header>
    // </div>
  );
}
