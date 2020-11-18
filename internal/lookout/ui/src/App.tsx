import React from 'react';
import { AppBar, Tabs, Tab, Toolbar, Typography } from '@material-ui/core'
import { BrowserRouter as Router, Link, Switch, Route} from 'react-router-dom'
import { JobService } from './services/jobs';
import { JobTableContainer } from "./containers/JobTableContainer";
import { Overview } from './Overview'
import './App.css';


export function App(services: { jobService: JobService; }) {
  const [value, setValue] = React.useState(0);

  const handleChange = (event: any, newValue: any) => {
    setValue(newValue);
  };

  return (
    <Router>
      <div className="wrapper">
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6">
              Armada Lookout
            </Typography>
            <Tabs value={value} onChange={handleChange}>
              <Tab label="Overview" component={Link} to="/"/>
              <Tab label="Jobs" component={Link} to="/jobs"/>
            </Tabs>
          </Toolbar>
        </AppBar>
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
