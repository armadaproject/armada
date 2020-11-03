import * as React from 'react';
import { App } from './app';
import { AppBar, Tabs, Tab, Toolbar, Typography } from '@material-ui/core'
import { BrowserRouter as Router, Link, Switch, Route } from 'react-router-dom'
import { Overview } from './overview'

export function Main({app}:{app: App}): JSX.Element {
    return (
        <>
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
                            <Overview jobs={app.jobs}/>
                        </Route>
                    </Switch>
                </div>
            </Router>
        </>
    );
}