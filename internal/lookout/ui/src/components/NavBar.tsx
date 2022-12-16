import React from "react"

import { AppBar, Tab, Tabs, Toolbar, Typography } from "@material-ui/core"
import { Link, RouteComponentProps, withRouter } from "react-router-dom"

import "./NavBar.css"

interface Page {
  title: string
  location: string
}

const PAGES: Page[] = [
  {
    title: "Overview",
    location: "/",
  },
  {
    title: "Job Sets",
    location: "/job-sets",
  },
  {
    title: "Jobs",
    location: "/jobs",
  },
]

// Creates mapping from location to index of element in ordered navbar
function getLocationMap(pages: Page[]): Map<string, number> {
  const locationMap = new Map<string, number>()
  pages.forEach((page, index) => {
    locationMap.set(page.location, index)
  })
  return locationMap
}

const locationMap = getLocationMap(PAGES)

function locationFromIndex(pages: Page[], index: number): string {
  if (pages[index]) {
    return pages[index].location
  }
  return "/"
}

function NavBar(props: RouteComponentProps) {
  const currentLocation = props.location.pathname
  const currentValue = locationMap.has(currentLocation) ? locationMap.get(currentLocation) : 0
  return (
    <AppBar position="static">
      <Toolbar className="toolbar">
        <a href="/" className="title">
          <img className="logo" src={process.env.PUBLIC_URL + "./Armada-white-rectangle.png"} alt={""} />
          <Typography variant="h6" className="app-name">
            Lookout
          </Typography>
        </a>
        <div className="nav-items">
          <Tabs
            value={currentValue}
            onChange={(event, newIndex) => {
              const newLocation = locationFromIndex(PAGES, newIndex)
              props.history.push(newLocation)
            }}
          >
            {PAGES.map((page, idx) => (
              <Tab key={idx} label={page.title} component={Link} to={page.location} />
            ))}
          </Tabs>
        </div>
      </Toolbar>
    </AppBar>
  )
}

export default withRouter(NavBar)
