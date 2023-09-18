import React from "react"

import {
  Container,
  IconButton,
  Menu,
  MenuItem,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core"
import MoreVert from "@material-ui/icons/MoreVert"
import { AutoSizer } from "react-virtualized"

import AutoRefreshToggle from "./AutoRefreshToggle"
import RefreshButton from "./RefreshButton"
import { QueueInfo } from "../services/JobService"
import { RequestStatus } from "../utils"

import "./Overview.css"

type OverviewProps = {
  queueInfos: QueueInfo[]
  openQueueMenu: string
  queueMenuAnchor: HTMLElement | null
  overviewRequestStatus: RequestStatus
  autoRefresh: boolean
  onRefresh: () => void
  onJobClick: (jobId: string, queue: string) => void
  onSetQueueMenu: (queue: string, anchor: HTMLElement | null) => void
  onQueueMenuJobSetsClick: (queue: string) => void
  onQueueMenuJobsClick: (queue: string) => void
  onToggleAutoRefresh: (autoRefresh: boolean) => void
}

export default function Overview(props: OverviewProps) {
  return (
    <Container className="overview">
      <div className="overview-header">
        <h2 className="title">Overview</h2>
        <div className="right">
          <div className="auto-refresh">
            <AutoRefreshToggle autoRefresh={props.autoRefresh} onAutoRefreshChange={props.onToggleAutoRefresh} />
          </div>
          <div className="refresh-button">
            <RefreshButton isLoading={props.overviewRequestStatus === "Loading"} onClick={props.onRefresh} />
          </div>
        </div>
      </div>
      <div className="overview-table">
        <AutoSizer>
          {({ height, width }) => {
            return (
              <div
                style={{
                  height: height,
                  width: width,
                }}
              >
                <TableContainer
                  component={Paper}
                  style={{
                    maxHeight: height,
                    overflowY: "auto",
                  }}
                >
                  <Table stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell>Queue</TableCell>
                        <TableCell align="right">Queued</TableCell>
                        <TableCell align="right">Pending</TableCell>
                        <TableCell align="right">Running</TableCell>
                        <TableCell align="center">Oldest Queued Job</TableCell>
                        <TableCell align="center">Longest Running Job</TableCell>
                        <TableCell className="details-button-cell" />
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {props.queueInfos.map((q) => (
                        <TableRow key={q.queue}>
                          <TableCell scope="row">{q.queue}</TableCell>
                          <TableCell align="right">{q.jobsQueued}</TableCell>
                          <TableCell align="right">{q.jobsPending}</TableCell>
                          <TableCell align="right">{q.jobsRunning}</TableCell>
                          <TableCell align="center" className="duration-cell">
                            <div
                              className={q.oldestQueuedJob ? "link" : ""}
                              onClick={() => {
                                if (q.oldestQueuedJob) {
                                  props.onJobClick(q.oldestQueuedJob.jobId, q.queue)
                                }
                              }}
                            >
                              {q.oldestQueuedDuration}
                            </div>
                          </TableCell>
                          <TableCell align="center" className="duration-cell">
                            <div
                              className={q.longestRunningJob ? "link" : ""}
                              onClick={() => {
                                if (q.longestRunningJob) {
                                  props.onJobClick(q.longestRunningJob.jobId, q.queue)
                                }
                              }}
                            >
                              {q.longestRunningDuration}
                            </div>
                          </TableCell>
                          <TableCell align="center" className="details-button-cell" padding="none">
                            <IconButton
                              title={"View in Jobs table"}
                              size="small"
                              onClick={(event) => {
                                props.onSetQueueMenu(q.queue, event.currentTarget)
                              }}
                            >
                              <MoreVert />
                            </IconButton>
                            <Menu
                              anchorEl={props.queueMenuAnchor}
                              open={q.queue === props.openQueueMenu}
                              onClose={() => props.onSetQueueMenu("", null)}
                            >
                              <MenuItem onClick={() => props.onQueueMenuJobSetsClick(q.queue)}>Job Sets</MenuItem>
                              <MenuItem onClick={() => props.onQueueMenuJobsClick(q.queue)}>Jobs</MenuItem>
                            </Menu>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </div>
            )
          }}
        </AutoSizer>
      </div>
    </Container>
  )
}
