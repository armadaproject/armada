import { Refresh } from "@mui/icons-material"
import { CircularProgress, IconButton } from "@mui/material"

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
          <Refresh />
        </IconButton>
      )}
    </div>
  )
}
