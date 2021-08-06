import React from "react"

import { Accordion, AccordionDetails, AccordionSummary, Table, TableBody, TableContainer } from "@material-ui/core"
import { ExpandMore } from "@material-ui/icons"

import { Job } from "../../services/JobService"
import { MakeJobDetailsRow, MakeJobDetailSpecifyKey } from "./JobDetailsUtils"
import { PreviousRuns } from "./PreviousRuns"
import { RunDetailsRows } from "./RunDetailsRows"

import "./Details.css"

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
            {MakeJobDetailsRow("Id", props.job.jobId)}
            {MakeJobDetailsRow("Queue", props.job.queue)}
            {MakeJobDetailsRow("Owner", props.job.owner)}
            {MakeJobDetailsRow("Job set", props.job.jobSet)}
            {MakeJobDetailsRow("Job state", props.job.jobState)}
            {MakeJobDetailsRow("Priority", props.job.priority?.toString())}
            {MakeJobDetailsRow("Submitted", props.job.submissionTime?.toString())}
            {props.job.cancelledTime && MakeJobDetailsRow("Cancelled", props.job.cancelledTime.toString())}
            {lastRun && <RunDetailsRows run={lastRun} />}
            {props.job.annotations &&
              Object.keys(props.job.annotations).map((annotationName) =>
                MakeJobDetailSpecifyKey(
                  "__annotation_" + annotationName,
                  annotationName,
                  props.job.annotations[annotationName],
                ),
              )}
          </TableBody>
        </Table>
      </TableContainer>
      {initRuns && (
        <PreviousRuns runs={initRuns} expandedItems={props.expandedItems} onToggleExpand={props.onToggleExpand} />
      )}
      {props.job.jobYaml && (
        <div className="details-yaml-container">
          <Accordion>
            <AccordionSummary expandIcon={<ExpandMore />}>
              <h3>Job YAML</h3>
            </AccordionSummary>
            <AccordionDetails>
              <p className="details-yaml">{props.job.jobYaml}</p>
            </AccordionDetails>
          </Accordion>
        </div>
      )}
    </div>
  )
}
