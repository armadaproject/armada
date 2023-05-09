import React, { useRef, useState } from "react"

import { IconButton } from "@material-ui/core"
import { Settings } from "@mui/icons-material"
import { Button, Popover, TextField, Typography } from "@mui/material"

import styles from "./CustomViewPicker.module.css"

export const CustomViewPicker = () => {
  const anchorElRef = useRef<HTMLDivElement>(null)
  const [isOpen, setIsOpen] = useState<boolean>(false)
  const [creatingCustomView, setCreatingCustomView] = useState<boolean>(false)
  const [newCustomViewName, setNewCustomViewName] = useState<string>("")

  const clearAddAnnotation = () => {
    setCreatingCustomView(false)
    setNewCustomViewName("")
  }

  return (
    <>
      <div
        style={{
          minWidth: "50px",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
        ref={anchorElRef}
      >
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
            <Typography display="block" variant="caption" sx={{ width: "100%" }}>
              Click here to create a custom view with your current configuration.
            </Typography>
            <div className={styles.addColumnButton}>
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
                  <div className={styles.addAnnotationButtons}>
                    <div className={styles.addAnnotationAction}>
                      <Button variant="outlined" onClick={clearAddAnnotation}>
                        Cancel
                      </Button>
                    </div>
                    <div className={styles.addAnnotationAction}>
                      <Button variant="contained" onClick={() => console.log("saving")}>
                        Save
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
        </div>
      </Popover>
    </>
  )
}
