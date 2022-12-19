import { Accordion, AccordionSummary, Typography, AccordionDetails } from "@material-ui/core"
import { ExpandMore } from "@mui/icons-material"
import { Job } from "models/lookoutV2Models"
import { formatJobRunState, formatUtcDate } from "utils/jobsTableFormatters"

import { KeyValuePairTable } from "./KeyValuePairTable"
export interface SidebarTabJobRuns {
  job: Job
}
export const SidebarTabJobRuns = ({ job }: SidebarTabJobRuns) => {
  const runsNewestFirst = [...job.runs].reverse()
  return (
    <>
      {runsNewestFirst.map((run) => {
        return (
          <Accordion key={run.runId}>
            <AccordionSummary expandIcon={<ExpandMore />} aria-controls="panel1a-content">
              <Typography>
                {formatUtcDate(run.pending)} UTC ({formatJobRunState(run.jobRunState)})
              </Typography>
            </AccordionSummary>
            <AccordionDetails>
              <KeyValuePairTable
                data={[
                  { key: "State", value: formatJobRunState(run.jobRunState) },
                  { key: "Run ID", value: run.runId },
                  { key: "Cluster", value: run.cluster },
                  { key: "Node", value: run.node ?? "" },
                  { key: "Started (UTC)", value: formatUtcDate(run.started) },
                  { key: "Pending (UTC)", value: formatUtcDate(run.pending) },
                  { key: "Finished (UTC)", value: formatUtcDate(run.finished) },
                  { key: "Exit code", value: run.exitCode?.toString() ?? "" },
                  { key: "Error info", value: run.error ?? "None" },
                ]}
              />
            </AccordionDetails>
          </Accordion>
        )
      })}
      {runsNewestFirst.length === 0 && <>This job has not run.</>}
    </>
  )
}
