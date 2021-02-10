import React from 'react'
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";

import { JobSet } from "../../services/JobService";

import "./JobSetTable.css"

interface JobSetTableProps {
  jobSets: JobSet[]
  onJobSetClick: (jobSet: string, state: string) => void
}

export default function JobSetTable(props: JobSetTableProps) {
  return (
    <TableContainer component={Paper} className="job-set-table">
      <Table aria-label="job-set-table" stickyHeader={true}>
        <TableHead>
          <TableRow>
            <TableCell>Job Set</TableCell>
            <TableCell align="right">Queued</TableCell>
            <TableCell align="right">Pending</TableCell>
            <TableCell align="right">Running</TableCell>
            <TableCell align="right">Succeeded</TableCell>
            <TableCell align="right">Failed</TableCell>
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
                    () => props.onJobSetClick(q.jobSet, "Queued") :
                    () => {
                    }}>
                  {q.jobsQueued}
                </div>
              </TableCell>
              <TableCell align="right">
                <div
                  className={q.jobsPending ? "link" : ""}
                  onClick={q.jobsPending ?
                    () => props.onJobSetClick(q.jobSet, "Pending") :
                    () => {
                    }}>
                  {q.jobsPending}
                </div>
              </TableCell>
              <TableCell align="right">
                <div
                  className={q.jobsRunning ? "link" : ""}
                  onClick={q.jobsRunning ?
                    () => props.onJobSetClick(q.jobSet, "Running") :
                    () => {
                    }}>
                  {q.jobsRunning}
                </div>
              </TableCell>
              <TableCell align="right">
                <div
                  className={q.jobsSucceeded ? "link" : ""}
                  onClick={q.jobsSucceeded ?
                    () => props.onJobSetClick(q.jobSet, "Succeeded") :
                    () => {
                    }}>
                  {q.jobsSucceeded}
                </div>
              </TableCell>
              <TableCell align="right">
                <div
                  className={q.jobsFailed ? "link" : ""}
                  onClick={q.jobsFailed ?
                    () => props.onJobSetClick(q.jobSet, "Failed") :
                    () => {
                    }}>
                  {q.jobsFailed}
                </div>
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </TableContainer>
  )
}
