import React from "react"

import { Checkbox, Input, InputLabel, ListItemText, MenuItem, MenuProps, Select } from "@material-ui/core"
import { TableHeaderProps } from "react-virtualized"

import { JOB_STATES_FOR_DISPLAY } from "../../services/JobService"

import "./JobStatesHeaderCell.css"

type JobStatesHeaderCellProps = {
  jobStates: string[]
  onJobStatesChange: (jobStates: string[]) => void
} & TableHeaderProps

const ITEM_HEIGHT = 64
const ITEM_PADDING_TOP = 8
const menuProps: Partial<MenuProps> = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT * JOB_STATES_FOR_DISPLAY.length + ITEM_PADDING_TOP,
      width: 250,
    },
  },
  anchorOrigin: {
    vertical: "bottom",
    horizontal: "left",
  },
  transformOrigin: {
    vertical: "top",
    horizontal: "left",
  },
  getContentAnchorEl: null,
}

export default function JobStatesHeaderCell(props: JobStatesHeaderCellProps) {
  return (
    <div className="job-states-header-cell">
      <InputLabel shrink={true} id="job-table-state-select-label">
        Job states
      </InputLabel>
      <Select
        labelId="job-table-state-select-label"
        id="job-table-state-select"
        multiple
        value={props.jobStates}
        onChange={(event) => {
          const newJobStates = (event.target.value as string[]).filter((jobState) =>
            JOB_STATES_FOR_DISPLAY.includes(jobState),
          )
          props.onJobStatesChange(newJobStates)
        }}
        input={<Input />}
        renderValue={(selected) => `${(selected as string[]).length} selected`}
        displayEmpty={true}
        MenuProps={menuProps}
        className="job-states-header-cell-select"
      >
        {JOB_STATES_FOR_DISPLAY.map((jobState) => (
          <MenuItem key={jobState} value={jobState}>
            <Checkbox checked={props.jobStates.indexOf(jobState) > -1} />
            <ListItemText primary={jobState} />
          </MenuItem>
        ))}
      </Select>
    </div>
  )
}
