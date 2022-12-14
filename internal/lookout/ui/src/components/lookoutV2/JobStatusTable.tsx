import { memo } from "react"

import { Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@mui/material"
import { Job } from "models/lookoutV2Models"

interface JobStatusTableProps {
  jobsToRender: Job[]
  totalJobCount: number
  additionalColumnsToDisplay: {
    displayName: string
    formatter: (job: Job) => string
  }[]
  showStatus: boolean
  jobStatus: Record<string, string>
}
export const JobStatusTable = memo(
  ({ jobsToRender, totalJobCount, additionalColumnsToDisplay, showStatus, jobStatus }: JobStatusTableProps) => {
    return (
      <TableContainer>
        <Table size="small" stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Job ID</TableCell>
              <TableCell>Queue</TableCell>
              <TableCell>Job Set</TableCell>
              {additionalColumnsToDisplay.map(({ displayName }) => (
                <TableCell key={displayName}>{displayName}</TableCell>
              ))}
              {showStatus && <TableCell>Status</TableCell>}
            </TableRow>
          </TableHead>

          <TableBody>
            {jobsToRender.map((job) => (
              <TableRow key={job.jobId}>
                <TableCell>{job.jobId}</TableCell>
                <TableCell>{job.queue}</TableCell>
                <TableCell>{job.jobSet}</TableCell>
                {additionalColumnsToDisplay.map((col) => (
                  <TableCell key={col.displayName}>{col.formatter(job)}</TableCell>
                ))}
                {showStatus && <TableCell>{jobStatus[job.jobId] ?? ""}</TableCell>}
              </TableRow>
            ))}
            {totalJobCount > jobsToRender.length && (
              <TableRow>
                <TableCell colSpan={5}>And {totalJobCount - jobsToRender.length} more jobs...</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </TableContainer>
    )
  },
)
