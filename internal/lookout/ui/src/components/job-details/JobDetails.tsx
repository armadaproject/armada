import React from "react"

import { Accordion, AccordionDetails, AccordionSummary, Table, TableBody, TableContainer } from "@material-ui/core"
import { ExpandMore } from "@material-ui/icons"

import { Job } from "../../services/JobService"
import DetailRow from "./DetailRow"
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
            <DetailRow name="Id" value={props.job.jobId} />
            <DetailRow name="Queue" value={props.job.queue} />
            <DetailRow name="Owner" value={props.job.owner} />
            <DetailRow name="Job set" value={props.job.jobSet} />
            <DetailRow name="Job state" value={props.job.jobState} />
            <DetailRow name="Priority" value={props.job.priority.toString()} />
            <DetailRow name="Submitted" value={props.job.submissionTime} />
            {props.job.cancelledTime && <DetailRow name="Cancelled" value={props.job.cancelledTime} />}
            {lastRun && <RunDetailsRows run={lastRun} />}
            {props.job.annotations &&
              Object.entries(props.job.annotations).map(([name, value]) => (
                <DetailRow key={"annotation-" + name} name={name} value={value} />
              ))}
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
