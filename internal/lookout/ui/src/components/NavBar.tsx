import { AppBar, Tab, Tabs, Toolbar, Typography } from "@mui/material"
import { Link } from "react-router-dom"

import { Router, withRouter } from "../utils"

import "./NavBar.css"

interface Page {
  title: string
  location: string
}

const PAGES: Page[] = [
  {
    title: "Jobs",
    location: "/",
  },
  {
    title: "Job Sets",
    location: "/job-sets",
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

interface NavBarProps {
  customTitle: string
  router: Router
  username?: string
}

function NavBar({ customTitle, router, username }: NavBarProps) {
  const currentLocation = router.location.pathname
  const currentValue = locationMap.has(currentLocation) ? locationMap.get(currentLocation) : 0
  return (
    <AppBar position="static">
      <Toolbar className="toolbar">
        <div>
          <a href="/" className="title">
            <img className="logo" src={import.meta.env.BASE_URL + "./Armada-white-rectangle.png"} alt={""} />
            <Typography variant="h6" className="app-name">
              Lookout
            </Typography>
            {customTitle && (
              <Typography variant="h5" className="app-name" style={{ paddingLeft: "3em" }}>
                {customTitle}
              </Typography>
            )}
          </a>
        </div>
        <div className="nav-items">
          <Tabs
            value={currentValue}
            onChange={(_, newIndex) => {
              const newLocation = locationFromIndex(PAGES, newIndex)
              router.navigate(newLocation)
            }}
            textColor="inherit"
            indicatorColor="secondary"
          >
            {PAGES.map((page, idx) => (
              <Tab key={idx} label={page.title} component={Link} to={page.location} />
            ))}
          </Tabs>
        </div>
        <div className="nav-end">
          <div>
            <Typography variant="h6" className="username" style={{ marginLeft: "auto" }}>
              {username ? <>Welcome, {username}!</> : <>Welcome!</>}
            </Typography>
          </div>
        </div>
      </Toolbar>
    </AppBar>
  )
}

export default withRouter(NavBar)
