import { Accordion, AccordionSummary, Typography, AccordionDetails } from "@material-ui/core"
import { ExpandMore } from "@mui/icons-material"
import { Job } from "models/lookoutV2Models"
import { formatUtcDate } from "utils/jobsTableFormatters"
import { KeyValuePairTable } from "./KeyValuePairTable"
export interface SidebarJobRunsProps {
  job: Job
}
export const SidebarJobRuns = ({ job }: SidebarJobRunsProps) => {
  const runsNewestFirst = job.runs.reverse()
  return (

    <>
      {runsNewestFirst.map(run => {
        return (
          <Accordion key={run.runId}>
            <AccordionSummary
              expandIcon={<ExpandMore />}
              aria-controls="panel1a-content"
            >
              <Typography>{formatUtcDate(run.pending)} UTC ({run.jobRunState})</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <KeyValuePairTable data={[
                {key: "State", value: run.jobRunState },
                {key: "Run ID", value: run.runId },
                {key: "Cluster", value: run.cluster },
                {key: "Node", value: run.node ?? "" },
                {key: "Pending (UTC)", value: formatUtcDate(run.pending) },
                {key: "Started (UTC)", value: formatUtcDate(run.started) },
                {key: "Finished (UTC)", value: formatUtcDate(run.finished) },
                {key: "Exit code", value: run.exitCode?.toString() ?? "" },
                {key: "Error info", value: run.error ?? "None" },
              ]}/>
            </AccordionDetails>
          </Accordion>
        )
      })}
      {runsNewestFirst.length === 0 && (
        <>This job has not run.</>
      )}
    </>
  )
}