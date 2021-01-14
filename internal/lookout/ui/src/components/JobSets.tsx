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
  onJobSetStatsClick: (jobSet: string, jobState: string) => void
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
                <TableCell align="right">
                  <div
                    className={q.jobsQueued ? "link" : ""}
                    onClick={q.jobsQueued ?
                      () => props.onJobSetStatsClick(q.jobSet, "Queued") :
                      () => {}}>
                    {q.jobsQueued}
                  </div>
                </TableCell>
                <TableCell align="right">
                  <div
                    className={q.jobsPending ? "link" : ""}
                    onClick={q.jobsPending ?
                      () => props.onJobSetStatsClick(q.jobSet, "Pending") :
                      () => {}}>
                    {q.jobsPending}
                  </div>
                </TableCell>
                <TableCell align="right">
                  <div
                    className={q.jobsRunning ? "link" : ""}
                    onClick={q.jobsRunning ?
                      () => props.onJobSetStatsClick(q.jobSet, "Running") :
                      () => {}}>
                    {q.jobsRunning}
                  </div>
                </TableCell>
                <TableCell align="right">
                  <div
                    className={q.jobsSucceeded ? "link" : ""}
                    onClick={q.jobsSucceeded ?
                      () => props.onJobSetStatsClick(q.jobSet, "Succeeded") :
                      () => {}}>
                    {q.jobsSucceeded}
                  </div>
                </TableCell>
                <TableCell align="right">
                  <div
                    className={q.jobsFailed ? "link" : ""}
                    onClick={q.jobsFailed ?
                      () => props.onJobSetStatsClick(q.jobSet, "Failed") :
                      () => {}}>
                    {q.jobsFailed}
                  </div>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  )
}
