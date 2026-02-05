import { forwardRef } from "react"

import { Settings } from "@mui/icons-material"
import { AppBar, Button, IconButton, Stack, styled, Toolbar, Typography } from "@mui/material"
import { Link, NavLink, NavLinkProps } from "react-router-dom"

import { SPACING } from "../common/spacing"
import { useUsername } from "../oidcAuth"
import { JOB_SETS, JOBS, SETTINGS } from "../pathnames"

import "./NavBar.css"

const NavLinkButton = forwardRef<HTMLAnchorElement, NavLinkProps>((props, ref) => (
  <NavLink {...props} style={({ isActive }) => (isActive ? undefined : { borderStyle: "none" })} ref={ref} />
))

const StyledAppBar = styled(AppBar)(({ theme }) => ({
  backgroundColor: theme.palette.appBar.main,
  backgroundImage: "unset",
  color: theme.palette.appBar.contrastText,
}))

interface Page {
  title: string
  location: string
}

const PAGES: Page[] = [
  {
    title: "Jobs",
    location: JOBS,
  },
  {
    title: "Job Sets",
    location: JOB_SETS,
  },
]

interface NavBarProps {
  customTitle: string
}

export const NavBar = ({ customTitle }: NavBarProps) => {
  const username = useUsername()

  return (
    <>
      <StyledAppBar position="static">
        <Toolbar className="toolbar">
          <div>
            <a href="/" className="title">
              <img className="logo" src={import.meta.env.BASE_URL + "./Armada-white-rectangle.png"} alt={""} />
              <Typography variant="h6">Lookout</Typography>
              {customTitle && (
                <Typography variant="h5" style={{ paddingLeft: "3em" }}>
                  {customTitle}
                </Typography>
              )}
            </a>
          </div>
          <Stack direction="row" alignItems="center" spacing={SPACING.sm}>
            {PAGES.map(({ location, title }) => (
              <Button
                key={location}
                variant="outlined"
                color="inherit"
                size="large"
                component={NavLinkButton}
                to={location}
              >
                {title}
              </Button>
            ))}
          </Stack>
          <div className="nav-end">
            <div>
              <Typography variant="h6" className="username" style={{ marginLeft: "auto" }}>
                {username ? <>Welcome, {username}!</> : <>Welcome!</>}
              </Typography>
            </div>
            <div>
              <IconButton aria-label="settings" size="large" color="inherit" component={Link} to={SETTINGS}>
                <Settings fontSize="inherit" />
              </IconButton>
            </div>
          </div>
        </Toolbar>
      </StyledAppBar>
    </>
  )
}

