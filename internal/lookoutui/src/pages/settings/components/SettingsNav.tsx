import { forwardRef } from "react"

import { AccountCircle, DisplaySettings, Info, Numbers, Palette } from "@mui/icons-material"
import { alpha, List, ListItemButton, ListItemIcon, ListItemText, useTheme } from "@mui/material"
import { NavLink, NavLinkProps } from "react-router-dom"

import {
  SETTINGS_ABOUT,
  SETTINGS_ACCOUNT,
  SETTINGS_APPEARANCE,
  SETTINGS_VALUE_DISPLAY,
  SETTINGS_VISUAL_THEME,
} from "../../../pathnames"

const ListItemButtonComponent = forwardRef<HTMLAnchorElement, NavLinkProps>((props, ref) => {
  const theme = useTheme()
  return (
    <NavLink
      {...props}
      ref={ref}
      style={({ isActive }) =>
        isActive
          ? {
              backgroundColor: alpha(theme.palette.primary.main, 0.3),
            }
          : undefined
      }
    />
  )
})

export const SettingsNav = () => (
  <List>
    <ListItemButton component={ListItemButtonComponent} to={SETTINGS_VISUAL_THEME}>
      <ListItemIcon>
        <Palette />
      </ListItemIcon>
      <ListItemText primary="Visual theme" />
    </ListItemButton>
    <ListItemButton component={ListItemButtonComponent} to={SETTINGS_VALUE_DISPLAY}>
      <ListItemIcon>
        <Numbers />
      </ListItemIcon>
      <ListItemText primary="Value display" />
    </ListItemButton>
    <ListItemButton component={ListItemButtonComponent} to={SETTINGS_APPEARANCE}>
      <ListItemIcon>
        <DisplaySettings />
      </ListItemIcon>
      <ListItemText primary="Appearance" />
    </ListItemButton>
    <ListItemButton component={ListItemButtonComponent} to={SETTINGS_ACCOUNT}>
      <ListItemIcon>
        <AccountCircle />
      </ListItemIcon>
      <ListItemText primary="Account" />
    </ListItemButton>
    <ListItemButton component={ListItemButtonComponent} to={SETTINGS_ABOUT}>
      <ListItemIcon>
        <Info />
      </ListItemIcon>
      <ListItemText primary="About" />
    </ListItemButton>
  </List>
)
