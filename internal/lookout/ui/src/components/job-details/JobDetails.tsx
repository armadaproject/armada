import React from 'react'
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
} from "@material-ui/core";

import { Job } from "../../services/JobService";
import { RunDetailsRows } from "./RunDetailsRows";
import { PreviousRuns } from "./PreviousRuns";

import './Details.css'

interface DetailsProps {
  job: Job
  expandedItems: Set<string>
  onToggleExpand: (k8sId: string, isExpanded: boolean) => void
}

export default function JobDetails(props: DetailsProps) {
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
            {lastRun && <RunDetailsRows run={lastRun} />}
          </TableBody>
        </Table>
      </TableContainer>
      {initRuns &&
      <PreviousRuns
        runs={initRuns}
        expandedItems={props.expandedItems}
        onToggleExpand={props.onToggleExpand}/>}
    </div>
  )
}
