import React from "react";
import {
  IconButton,
  MenuItem,
  TextField,
  Select,
  InputLabel,
  Input,
  Checkbox,
  ListItemText,
  MenuProps
} from "@material-ui/core";
import RefreshIcon from '@material-ui/icons/Refresh';

import { JOB_STATES_FOR_DISPLAY } from "../services/JobService";
import './JobTableHeader.css'

type JobTableHeaderProps = {
  queue: string
  jobSet: string
  jobStates: string[]
  newestFirst: boolean
  onQueueChange: (queue: string) => void
  onJobSetChange: (jobSet: string) => void
  onJobStatesChange: (jobStates: string[]) => void
  onOrderChange: (newestFirst: boolean) => void
  onRefresh: () => void
}

const ITEM_HEIGHT = 64;
const ITEM_PADDING_TOP = 8;
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
};

export default function JobTableHeader(props: JobTableHeaderProps) {
  return (
    <div className="job-table-header">
      <div className="left">
        <h2 className="title default-horizontal-margin">Jobs</h2>
      </div>
      <div className="center">
        <div className="default-horizontal-margin">
          <TextField
            value={props.queue}
            onChange={(event) => {
              props.onQueueChange(event.target.value)
            }}
            label="Queue"
            variant="outlined"/>
        </div>
        <div className="default-horizontal-margin">
          <TextField
            value={props.jobSet}
            onChange={(event) => {
              props.onJobSetChange(event.target.value)
            }}
            label="Job set"
            variant="filled"/>
        </div>
        <div className="default-horizontal-margin">
          <InputLabel id="job-table-state-select-label">Job states</InputLabel>
          <Select
            labelId="job-table-state-select-label"
            id="job-table-state-select"
            multiple
            value={props.jobStates}
            onChange={(event) => {
              const newJobStates = (event.target.value as string[])
                .filter(jobState => (JOB_STATES_FOR_DISPLAY).includes(jobState))
              props.onJobStatesChange(newJobStates)
            }}
            input={<Input/>}
            renderValue={(selected) => `${(selected as string[]).length} selected`}
            displayEmpty={true}
            MenuProps={menuProps}
          >
            {JOB_STATES_FOR_DISPLAY.map(jobState => (
              <MenuItem key={jobState} value={jobState}>
                <Checkbox checked={props.jobStates.indexOf(jobState) > -1}/>
                <ListItemText primary={jobState}/>
              </MenuItem>
            ))}
          </Select>
        </div>
        <div className="order-width default-horizontal-margin">
          <InputLabel id="job-table-order-select-label">Order</InputLabel>
          <Select
            className="order-width"
            labelId="job-table-order-select-label"
            value={props.newestFirst ? 1 : 0}
            onChange={event => {
              props.onOrderChange(event.target.value === 1)
            }}>
            <MenuItem value={1}>Newest</MenuItem>
            <MenuItem value={0}>Oldest</MenuItem>
          </Select>
        </div>
      </div>
      <div className="right">
        <div className="default-horizontal-margin">
          <IconButton onClick={props.onRefresh} color="primary">
            <RefreshIcon/>
          </IconButton>
        </div>
      </div>
    </div>
  )
}
