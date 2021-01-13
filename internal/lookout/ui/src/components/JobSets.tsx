import React from 'react'
import {
  Container,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow, TextField
} from "@material-ui/core";
import RefreshIcon from "@material-ui/icons/Refresh"

import { JobSet } from "../services/JobService";

import './JobSets.css'

interface JobSetsProps {
  queue: string
  jobSets: JobSet[]
  onQueueChange: (queue: string) => void
  onRefresh: () => void
}

export default function JobSets(props: JobSetsProps) {
  return (
    <Container className="job-sets">
      <TableContainer component={Paper}>
        <div className="job-sets-header">
          <h2 className="title">Job Sets</h2>
          <TextField
            value={props.queue}
            onChange={(event) => {
              props.onQueueChange(event.target.value)
            }}
            label="Queue"
            variant="outlined"/>
          <div className="refresh-button">
            <IconButton
              title={"Refresh"}
              onClick={props.onRefresh}
              color={"primary"}>
              <RefreshIcon/>
            </IconButton>
          </div>
        </div>
        <Table aria-label="simple table">
          <TableHead>
            <TableRow>
              <TableCell>Job Set</TableCell>
              <TableCell align="right">Queued</TableCell>
              <TableCell align="right">Pending</TableCell>
              <TableCell align="right">Running</TableCell>
              <TableCell align="right">Succeeded</TableCell>
              <TableCell align="right">Failed</TableCell>
              <TableCell className="details-button-cell"/>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.jobSets.map(q =>
              <TableRow key={q.jobSet}>
                <TableCell component="th" scope="row">
                  {q.jobSet}
                </TableCell>
                <TableCell align="right">{q.jobsQueued}</TableCell>
                <TableCell align="right">{q.jobsPending}</TableCell>
                <TableCell align="right">{q.jobsRunning}</TableCell>
                <TableCell align="right">{q.jobsSucceeded}</TableCell>
                <TableCell align="right">{q.jobsFailed}</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  )
}
