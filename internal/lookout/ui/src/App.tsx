import React from 'react';
import { BrowserRouter as Router, Switch, Route} from 'react-router-dom'
import { JobService } from './services/jobs';
import { JobTableContainer } from "./containers/JobTableContainer";
import { Overview } from './Overview'
import './App.css';
import NavBar from "./components/NavBar";

export function App(services: { jobService: JobService; }) {
  return (
    <Router>
      <div className="wrapper">
        <NavBar />
        <div className="content">
          <Switch>
            <Route path="/jobs">
              <JobTableContainer jobService={services.jobService} />
            </Route>
            <Route path="/">
              <Overview jobService={services.jobService} />
            </Route>
          </Switch>
        </div>
      </div>
    </Router>
  );
}
