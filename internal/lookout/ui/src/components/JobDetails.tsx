import React, { Fragment } from 'react'

import {
  Collapse,
  Container,
  IconButton,
  Paper,
  List,
  ListItem,
  ListItemText,
  TableContainer,
  Table,
  TableBody,
  TableRow,
  TableCell,
  TextField,
} from "@material-ui/core";
import RefreshIcon from "@material-ui/icons/Refresh";
import { ExpandMore, ExpandLess } from "@material-ui/icons";

import { Job, Run } from "../services/JobService";

import './JobDetails.css'

interface JobDetailsProps {
  jobId: string
  job?: Job
  expandedItems: Set<string>
  onJobIdChange: (jobId: string) => void
  onRefresh: () => void
  onToggleExpand: (k8sId: string, isExpanded: boolean) => void
}

interface DetailsProps {
  job: Job
  expandedItems: Set<string>
  onToggleExpand: (k8sId: string, isExpanded: boolean) => void
}

interface RunDetailsRowsProps {
  run: Run
}

function RunDetailsRows(props: RunDetailsRowsProps) {
  return (
    <Fragment>
      <TableRow className="field">
        <TableCell className="field-label">Cluster</TableCell>
        <TableCell>{props.run.cluster}</TableCell>
      </TableRow>
      <TableRow className="field">
        <TableCell className="field-label">Kubernetes Id</TableCell>
        <TableCell>{props.run.k8sId}</TableCell>
      </TableRow>
      {props.run.node &&
      <TableRow className="field">
        <TableCell className="field-label">Cluster node</TableCell>
        <TableCell>{props.run.node}</TableCell>
      </TableRow>}
      {props.run.podCreationTime &&
      <TableRow className="field">
        <TableCell className="field-label">Scheduled on cluster</TableCell>
        <TableCell>{props.run.podCreationTime}</TableCell>
      </TableRow>}
      {props.run.podStartTime &&
      <TableRow className="field">
        <TableCell className="field-label">Job started</TableCell>
        <TableCell>{props.run.podStartTime}</TableCell>
      </TableRow>}
      {props.run.finishTime &&
      <TableRow className="field">
        <TableCell className="field-label">Finished</TableCell>
        <TableCell>{props.run.finishTime}</TableCell>
      </TableRow>}
      {props.run.error &&
      <TableRow className="field">
        <TableCell className="field-label">Error</TableCell>
        <TableCell><span className="error-message">{props.run.error}</span></TableCell>
      </TableRow>}
    </Fragment>
  )
}

function Details(props: DetailsProps) {
  const lastRun = props.job.runs.length > 0 ? props.job.runs[props.job.runs.length - 1] : null
  const initRuns = props.job.runs.length > 1 ? props.job.runs.slice(0, -1).reverse() : null

  return (
    <div className="content">
      <TableContainer>
        <Table>
          <colgroup>
            <col style={{ width: '30%' }}/>
            <col style={{ width: '70%' }}/>
          </colgroup>
          <TableBody>
            <TableRow>
              <TableCell className="field-label">Id</TableCell>
              <TableCell>{props.job.jobId}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Queue</TableCell>
              <TableCell>{props.job.queue}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Owner</TableCell>
              <TableCell>{props.job.owner}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Job set</TableCell>
              <TableCell>{props.job.jobSet}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Job state</TableCell>
              <TableCell>{props.job.jobState}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Priority</TableCell>
              <TableCell>{props.job.priority}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Submitted</TableCell>
              <TableCell>{props.job.submissionTime}</TableCell>
            </TableRow>
            {props.job.cancelledTime &&
            <TableRow>
              <TableCell className="field-label">Cancelled</TableCell>
              <TableCell>{props.job.cancelledTime}</TableCell>
            </TableRow>}
            {lastRun && <RunDetailsRows run={lastRun}/>}
          </TableBody>
        </Table>
      </TableContainer>
      {initRuns &&
      <Fragment>
        <h3 className="scheduling-history-title">Scheduling history</h3>
        <div className="scheduling-history">
          <List
            component={Paper}>
            {initRuns && initRuns.map(run => (
              <Fragment key={run.k8sId}>
                <ListItem key={run.k8sId + "-0"} button onClick={() => {
                  if (props.expandedItems.has(run.k8sId)) {
                    props.onToggleExpand(run.k8sId, false)
                  } else {
                    props.onToggleExpand(run.k8sId, true)
                  }
                }}>
                  <ListItemText>{run.cluster}</ListItemText>
                  {props.expandedItems.has(run.k8sId) ? <ExpandLess /> : <ExpandMore />}
                </ListItem>
                <Collapse key={run.k8sId + "-1"} in={props.expandedItems.has(run.k8sId)} timeout="auto" unmountOnExit>
                  <div className="nested-run">
                    <TableContainer>
                      <Table>
                        <TableBody>
                          <RunDetailsRows run={run}/>
                        </TableBody>
                      </Table>
                    </TableContainer>
                  </div>
                </Collapse>
              </Fragment>
            ))}
          </List>
        </div>
      </Fragment>}
    </div>
  )
}

export default function JobDetails(props: JobDetailsProps) {
  return (
    <Container className="job-details">
      <Paper>
        <div className="job-details-header">
          <h2 className="title">Job details</h2>
          <TextField
            value={props.jobId}
            onChange={(event) => {
              props.onJobIdChange(event.target.value)
            }}
            label="Id"
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
        {props.job &&
        <Details
          job={props.job}
          expandedItems={props.expandedItems}
          onToggleExpand={props.onToggleExpand} />}
      </Paper>
    </Container>
  )
}
