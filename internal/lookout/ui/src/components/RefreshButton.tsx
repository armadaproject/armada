import React from "react"

import { CircularProgress, IconButton } from "@material-ui/core"
import RefreshIcon from "@material-ui/icons/Refresh"

import "./RefreshButton.css"

type RefreshButtonProps = {
  isLoading: boolean
  onClick: () => void
}

export default function RefreshButton(props: RefreshButtonProps) {
  return (
    <div className="refresh">
      {props.isLoading ? (
        <CircularProgress size={20} />
      ) : (
        <IconButton title={"Refresh"} onClick={props.onClick} color={"primary"} size="small">
          <RefreshIcon />
        </IconButton>
      )}
    </div>
  )
}
