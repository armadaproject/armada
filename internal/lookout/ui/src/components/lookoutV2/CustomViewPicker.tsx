import React, { useRef, useState } from "react"

import { Delete, Settings } from "@mui/icons-material"
import {
  Button,
  Popover,
  TextField,
  Typography,
  IconButton,
  List,
  ListItem,
  ListItemText,
  ListItemButton,
} from "@mui/material"

import styles from "./CustomViewPicker.module.css"

interface CustomViewPickerProps {
  customViews: string[]
  onAddCustomView: (name: string) => void
  onDeleteCustomView: (name: string) => void
  onLoadCustomView: (name: string) => void
}

export const CustomViewPicker = ({
  customViews,
  onAddCustomView,
  onDeleteCustomView,
  onLoadCustomView,
}: CustomViewPickerProps) => {
  const anchorElRef = useRef<HTMLDivElement>(null)
  const [isOpen, setIsOpen] = useState<boolean>(false)
  const [creatingCustomView, setCreatingCustomView] = useState<boolean>(false)
  const [newCustomViewName, setNewCustomViewName] = useState<string>("")

  const clearAddCustomView = () => {
    setCreatingCustomView(false)
    setNewCustomViewName("")
  }

  const addCustomView = () => {
    if (newCustomViewName === "") {
      return
    }
    onAddCustomView(newCustomViewName)
    clearAddCustomView()
  }

  return (
    <>
      <div className={styles.settingsButton} ref={anchorElRef}>
        <IconButton title={"Customize"} color={"primary"} size="small" onClick={() => setIsOpen(!isOpen)}>
          <Settings />
        </IconButton>
      </div>
      <Popover
        open={isOpen}
        onClose={() => setIsOpen(false)}
        anchorEl={anchorElRef.current}
        anchorOrigin={{
          vertical: "bottom",
          horizontal: "center",
        }}
        transformOrigin={{
          vertical: "top",
          horizontal: "center",
        }}
      >
        <div className={styles.customViewPickerContainer}>
          <div className={styles.createCustomViewContainer}>
            <Typography display="block" variant="caption" sx={{ width: "100%", paddingBottom: "10px" }}>
              Click here to create a custom view with your current configuration.
            </Typography>
            <div className={styles.addCustomViewButton}>
              {creatingCustomView ? (
                <>
                  <TextField
                    variant="outlined"
                    label="Custom View Name"
                    size="small"
                    sx={{ width: "100%" }}
                    autoFocus
                    value={newCustomViewName}
                    onChange={(e) => {
                      setNewCustomViewName(e.target.value)
                    }}
                    onKeyUp={(e) => {
                      if (e.key === "Enter") {
                        console.log("Saving")
                      }
                    }}
                  />
                  <div className={styles.addCustomViewActions}>
                    <div className={styles.addCustomViewAction}>
                      <Button variant="contained" onClick={addCustomView}>
                        Save
                      </Button>
                    </div>
                    <div className={styles.addCustomViewAction}>
                      <Button variant="outlined" onClick={clearAddCustomView}>
                        Cancel
                      </Button>
                    </div>
                  </div>
                </>
              ) : (
                <Button variant="contained" onClick={() => setCreatingCustomView(true)}>
                  Create Custom View
                </Button>
              )}
            </div>
          </div>
          <div>
            <List>
              {customViews
                .slice()
                .reverse()
                .map((name) => (
                  <ListItem
                    disablePadding
                    key={name}
                    secondaryAction={
                      <IconButton edge="end" aria-label="delete" onClick={() => onDeleteCustomView(name)}>
                        <Delete />
                      </IconButton>
                    }
                  >
                    <ListItemButton onClick={() => onLoadCustomView(name)}>
                      <ListItemText>{name}</ListItemText>
                    </ListItemButton>
                  </ListItem>
                ))}
            </List>
          </div>
        </div>
      </Popover>
    </>
  )
}
