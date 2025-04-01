import { Cancel, LowPriority } from "@mui/icons-material"
import { Button, Container, FormControlLabel, Checkbox, Tooltip, styled } from "@mui/material"

import { JobSet, JobSetsOrderByColumn } from "../../services/JobService"
import { RequestStatus } from "../../utils"
import AutoRefreshToggle from "../AutoRefreshToggle"
import RefreshButton from "../RefreshButton"
import JobSetTable from "./JobSetTable"
import "./JobSets.css"
import { QueueSelector } from "./QueueSelector"

const HeaderStartContainer = styled("div")({
  display: "flex",
  gap: "1em",
  alignItems: "center",

  "> *": {
    flexShrink: "0",
  },
})

interface JobSetsProps {
  queue: string
  jobSets: JobSet[]
  selectedJobSets: Map<string, JobSet>
  canCancel: boolean
  getJobSetsRequestStatus: RequestStatus
  autoRefresh: boolean
  canReprioritize: boolean
  orderByColumn: JobSetsOrderByColumn
  orderByDesc: boolean
  activeOnly: boolean
  onQueueChange: (queue: string) => void
  onRefresh: () => void
  onSelectJobSet: (index: number, selected: boolean) => void
  onShiftSelectJobSet: (index: number, selected: boolean) => void
  onDeselectAllClick: () => void
  onSelectAllClick: () => void
  onCancelJobSetsClick: () => void
  onToggleAutoRefresh: ((autoRefresh: boolean) => void) | undefined
  onReprioritizeJobSetsClick: () => void
  onOrderChange: (orderByColumn: JobSetsOrderByColumn, orderByDesc: boolean) => void
  onActiveOnlyChange: (activeOnly: boolean) => void
  onJobSetStateClick(rowIndex: number, state: string): void
}

export default function JobSets(props: JobSetsProps) {
  return (
    <Container className="job-sets" maxWidth={false}>
      <div className="job-sets-header">
        <HeaderStartContainer>
          <div>
            <h2 className="title">Job Sets</h2>
          </div>
          <div>
            <QueueSelector value={props.queue} onChange={props.onQueueChange} />
          </div>
          <div className="job-sets-field">
            <Tooltip title="Only display job sets with at least one active job.">
              <FormControlLabel
                control={
                  <Checkbox
                    color="primary"
                    checked={props.activeOnly}
                    onChange={(event) => {
                      props.onActiveOnlyChange(event.target.checked)
                    }}
                  />
                }
                label="Active only"
                labelPlacement="end"
              />
            </Tooltip>
          </div>
        </HeaderStartContainer>
        <div className="job-sets-actions">
          <div className="reprioritize-button">
            <Button
              disabled={!props.canReprioritize}
              variant="contained"
              color="primary"
              startIcon={<LowPriority />}
              onClick={props.onReprioritizeJobSetsClick}
            >
              Reprioritize
            </Button>
          </div>
          <div className="cancel-button">
            <Button
              disabled={!props.canCancel}
              variant="contained"
              color="secondary"
              startIcon={<Cancel />}
              onClick={props.onCancelJobSetsClick}
            >
              Cancel
            </Button>
          </div>
          {props.onToggleAutoRefresh && (
            <div className="auto-refresh">
              <AutoRefreshToggle autoRefresh={props.autoRefresh} onAutoRefreshChange={props.onToggleAutoRefresh} />
            </div>
          )}
          <div className="refresh-button">
            <RefreshButton isLoading={props.getJobSetsRequestStatus === "Loading"} onClick={props.onRefresh} />
          </div>
        </div>
      </div>
      <div className="job-sets-content">
        <JobSetTable
          queue={props.queue}
          jobSets={props.jobSets}
          selectedJobSets={props.selectedJobSets}
          orderByColumn={props.orderByColumn}
          orderByDesc={props.orderByDesc}
          onSelectJobSet={props.onSelectJobSet}
          onShiftSelectJobSet={props.onShiftSelectJobSet}
          onDeselectAllClick={props.onDeselectAllClick}
          onSelectAllClick={props.onSelectAllClick}
          onOrderChange={props.onOrderChange}
          onJobSetStateClick={props.onJobSetStateClick}
        />
      </div>
    </Container>
  )
}
