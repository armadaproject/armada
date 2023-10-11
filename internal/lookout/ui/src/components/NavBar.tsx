import React from "react"

import { AppBar, Toolbar, Typography } from "@material-ui/core"

import { Router, withRouter } from "../utils"

import "./NavBar.css"

interface NavBarProps {
  customTitle: string
  router: Router
}

function NavBar({ customTitle }: NavBarProps) {
  return (
    <AppBar position="static">
      <Toolbar className="toolbar">
        <a href="/" className="title">
          <img className="logo" src={process.env.PUBLIC_URL + "./Armada-white-rectangle.png"} alt={""} />
          <Typography variant="h6" className="app-name">
            Lookout
          </Typography>
          {customTitle && (
            <Typography variant="h5" className="app-name" style={{ paddingLeft: "3em" }}>
              {customTitle}
            </Typography>
          )}
        </a>
      </Toolbar>
    </AppBar>
  )
}

export default withRouter(NavBar)
