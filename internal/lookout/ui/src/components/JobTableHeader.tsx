import React from "react";
import { IconButton, MenuItem, TextField, Select, InputLabel } from "@material-ui/core";
import RefreshIcon from '@material-ui/icons/Refresh';

import './JobTableHeader.css'

type JobTableHeaderProps = {
  queue: string
  newestFirst: boolean
  onQueueChange: (queue: string) => void
  onOrderChange: (newestFirst: boolean) => void
  onRefresh: () => void
}

export default function JobTableHeader(props: JobTableHeaderProps) {
  return (
    <div className="job-table-header">
      <div className="left">
        <h2 className="title">Jobs</h2>
      </div>
      <div className="center">
        <div className="search">
          <TextField
            value={props.queue}
            onChange={(event) => {
              props.onQueueChange(event.target.value)
            }}
            label="Queue"
            variant="outlined"/>
        </div>
        <div className="order">
          <InputLabel id="job-table-order-select-label">Order</InputLabel>
          <Select
            className="select-field"
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
        <div className="refresh">
          <IconButton onClick={props.onRefresh} color="primary">
            <RefreshIcon/>
          </IconButton>
        </div>
      </div>
    </div>
  )
}
