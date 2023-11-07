import React from "react"

import { Switch, FormControlLabel } from "@material-ui/core"

type AutoRefreshToggle = {
  autoRefresh: boolean
  onAutoRefreshChange: (autoRefresh: boolean) => void
}

export default function AutoRefreshToggle(props: AutoRefreshToggle) {
  return (
    <div>
      <FormControlLabel
        control={
          <Switch
            checked={props.autoRefresh}
            onChange={(event) => {
              props.onAutoRefreshChange(event.target.checked)
            }}
            color="primary"
          />
        }
        label="Auto refresh"
        labelPlacement="start"
      />
    </div>
  )
}
