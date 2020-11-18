import React from 'react'
import { AppBar, Tab, Tabs, Toolbar, Typography } from "@material-ui/core";
import { Link } from "react-router-dom";

export type NavPage = "Overview" | "Jobs"

type NavBarProps = {
  currentPage: NavPage
  onPageChange: (page: NavPage) => void
}

const pageValueMap = new Map<NavPage, number>()
pageValueMap.set("Overview", 0)
pageValueMap.set("Jobs", 1)

const valuePageMap = new Map<number, NavPage>()
valuePageMap.set(0, "Overview")
valuePageMap.set(1, "Jobs")


export function NavBar(props: NavBarProps) {
  return (
    <AppBar position="static">
      <Toolbar>
        <Typography variant="h6">
          Armada Lookout
        </Typography>
        <Tabs value={pageValueMap.get(props.currentPage)} onChange={(event, newValue) => {
          props.onPageChange(valuePageMap.get(newValue) || "Overview")
        }}>
          <Tab label="Overview" component={Link} to="/"/>
          <Tab label="Jobs" component={Link} to="/jobs"/>
        </Tabs>
      </Toolbar>
    </AppBar>
  )
}
