import React, { useEffect, useState } from "react"

import { Accordion, AccordionDetails, AccordionSummary, Table, TableBody, TableContainer } from "@material-ui/core"
import { ExpandMore } from "@material-ui/icons"

import {
  convertStringToYaml,
  getCommandFromJobYaml,
  getCommandArgumentsFromJobYaml,
  getCpuFromJobYaml,
  getMemoryFromJobYaml,
  getGpuFromJobYaml,
  getStorageFromJobYaml,
} from "../../services/ComputeResourcesService"
import { Job } from "../../services/JobService"
import DetailRow from "./DetailRow"
import { PreviousRuns } from "./PreviousRuns"
import RunDetailsRows from "./RunDetailsRows"

import "./Details.css"

type ToggleFn = (item: string, isExpanded: boolean) => void
type DetailsProps = {
  job: Job
}

export function useExpanded(): [Set<string>, ToggleFn, () => void] {
  const [expandedItems, setExpandedItems] = useState<Set<string>>(new Set())

  function toggle(item: string, isExpanded: boolean) {
    const newExpanded = new Set<string>(expandedItems)
    if (isExpanded) {
      newExpanded.add(item)
    } else {
      newExpanded.delete(item)
    }
    setExpandedItems(newExpanded)
  }

  function clear() {
    setExpandedItems(new Set<string>())
  }

  return [expandedItems, toggle, clear]
}

export default function JobDetails(props: DetailsProps) {
  const [expandedItems, toggleExpanded, clearExpanded] = useExpanded()

  useEffect(() => {
    return () => {
      clearExpanded()
    }
  }, [props.job])

  const lastRun = props.job.runs.length > 0 ? props.job.runs[props.job.runs.length - 1] : null
  const initRuns = props.job.runs.length > 1 ? props.job.runs.slice(0, -1).reverse() : null
  const jobYaml = convertStringToYaml(props.job.jobYaml)
  const cpuResources = getCpuFromJobYaml(jobYaml)
  const gpuResources = getGpuFromJobYaml(jobYaml)
  const memoryResources = getMemoryFromJobYaml(jobYaml)
  const diskResources = getStorageFromJobYaml(jobYaml)
  const command = getCommandFromJobYaml(jobYaml)
  const commandArgs = getCommandArgumentsFromJobYaml(jobYaml)
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
            {command &&
              command.map(
                (values, index) =>
                  values.length > 0 && <DetailRow key={"Command-" + index} name={"Command-" + index} value={values} />,
              )}
            {commandArgs &&
              commandArgs.map(
                (values, index) =>
                  values.length > 0 && (
                    <DetailRow key={"Arguments-" + index} name={"Arguments-" + index} value={values} />
                  ),
              )}
            {cpuResources &&
              cpuResources.map(
                (values, index) =>
                  values.length > 0 && <DetailRow key={"CPU-" + index} name={"CPU-" + index} value={values} />,
              )}
            {gpuResources &&
              gpuResources.map(
                (values, index) =>
                  values.length > 0 && <DetailRow key={"GPU-" + index} name={"GPU-" + index} value={values} />,
              )}
            {memoryResources &&
              memoryResources.map(
                (values, index) =>
                  values.length > 0 && <DetailRow key={"Memory-" + index} name={"Memory-" + index} value={values} />,
              )}
            {diskResources &&
              diskResources.map(
                (values, index) =>
                  values.length > 0 && <DetailRow key={"Disk-" + index} name={"Disk-" + index} value={values} />,
              )}
            {props.job.cancelledTime && <DetailRow name="Cancelled" value={props.job.cancelledTime} />}
            {lastRun && <RunDetailsRows run={lastRun} jobId={props.job.jobId} />}
            {props.job.annotations &&
              Object.entries(props.job.annotations).map(([name, value]) => (
                <DetailRow
                  key={"annotation-" + name}
                  detailRowKey={"annotation-" + name}
                  isAnnotation
                  name={name}
                  value={value}
                />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      {initRuns && (
        <PreviousRuns
          runs={initRuns}
          jobId={props.job.jobId}
          expandedItems={expandedItems}
          onToggleExpand={toggleExpanded}
        />
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
