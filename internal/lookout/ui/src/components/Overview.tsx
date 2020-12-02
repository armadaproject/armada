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
  TableRow
} from "@material-ui/core";
import ViewListIcon from "@material-ui/icons/ViewList"
import RefreshIcon from "@material-ui/icons/Refresh"

import { QueueInfo } from "../services/JobService";
import './Overview.css'

interface OverviewProps {
  queueInfos: QueueInfo[]
  onQueueInfoClick: (queue: string) => void
  onRefresh: () => void
}

export default function Overview(props: OverviewProps) {
  return (
    <Container className="overview">
      <TableContainer component={Paper}>
        <div className="overview-header">
          <h2 className="title">Overview</h2>
          <div className="refresh-button">
            <IconButton
              title={"Refresh"}
              onClick={props.onRefresh}
              color={"primary"}>
              <RefreshIcon />
            </IconButton>
          </div>
        </div>
        <Table aria-label="simple table">
          <TableHead>
            <TableRow>
              <TableCell>Queue</TableCell>
              <TableCell align="right">Queued</TableCell>
              <TableCell align="right">Pending</TableCell>
              <TableCell align="right">Running</TableCell>
              <TableCell className="details-button-cell" />
            </TableRow>
          </TableHead>
          <TableBody>
            {props.queueInfos.map(q =>
              <TableRow key={q.queue}>
                <TableCell component="th" scope="row">
                  {q.queue}
                </TableCell>
                <TableCell align="right">{q.jobsQueued}</TableCell>
                <TableCell align="right">{q.jobsPending}</TableCell>
                <TableCell align="right">{q.jobsRunning}</TableCell>
                <TableCell align="right" className="details-button-cell">
                  <IconButton
                    title={"View in Jobs table"}
                    onClick={() => props.onQueueInfoClick(q.queue)}>
                    <ViewListIcon />
                  </IconButton>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </Container>
  )
}
