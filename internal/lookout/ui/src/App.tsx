import React from 'react';
import { BrowserRouter as Router, Switch, Route} from 'react-router-dom'

import JobService from './services/JobService';
import { JobsContainer } from "./containers/JobsContainer";
import { Overview } from './Overview'
import NavBar from "./components/NavBar";

import './App.css';

export function App(services: { jobService: JobService; }) {
  return (
    <Router>
      <div className="wrapper">
        <NavBar />
        <div className="content">
          <Switch>
            <Route exact path="/">
              <Overview jobService={services.jobService} />
            </Route>
            <Route exact path="/jobs">
              <JobsContainer jobService={services.jobService} />
            </Route>
          </Switch>
        </div>
      </div>
    </Router>
  );
}
