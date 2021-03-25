import React from 'react'
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableRow,
} from "@material-ui/core";
import { ExpandMore } from "@material-ui/icons";

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
    <div className="details-content">
      <TableContainer>
        <Table className="details-table-container">
          <TableBody>
            <TableRow>
              <TableCell className="field-label">Id</TableCell>
              <TableCell className="field-value">{props.job.jobId}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Queue</TableCell>
              <TableCell className="field-value">{props.job.queue}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Owner</TableCell>
              <TableCell className="field-value">{props.job.owner}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Job set</TableCell>
              <TableCell className="field-value">{props.job.jobSet}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Job state</TableCell>
              <TableCell className="field-value">{props.job.jobState}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Priority</TableCell>
              <TableCell className="field-value">{props.job.priority}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="field-label">Submitted</TableCell>
              <TableCell className="field-value">{props.job.submissionTime}</TableCell>
            </TableRow>
            {props.job.cancelledTime &&
            <TableRow>
              <TableCell className="field-label">Cancelled</TableCell>
              <TableCell className="field-value">{props.job.cancelledTime}</TableCell>
            </TableRow>}
            {lastRun && <RunDetailsRows run={lastRun}/>}
          </TableBody>
        </Table>
      </TableContainer>
      {initRuns &&
      <PreviousRuns
        runs={initRuns}
        expandedItems={props.expandedItems}
        onToggleExpand={props.onToggleExpand}/>}
      {props.job.jobYaml &&
      <div className="details-yaml-container">
        <Accordion>
          <AccordionSummary
            expandIcon={<ExpandMore/>}>
            <h3>Show Job YAML</h3>
          </AccordionSummary>
          <AccordionDetails>
            <p className="details-yaml">
              {props.job.jobYaml}
            </p>
          </AccordionDetails>
        </Accordion>
      </div>}
    </div>
  )
}
