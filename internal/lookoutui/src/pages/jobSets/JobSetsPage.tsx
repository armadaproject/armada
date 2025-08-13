import { IGroupJobsService } from "../../services/lookout/GroupJobsService"
import { UpdateJobSetsService } from "../../services/lookout/UpdateJobSetsService"

import JobSetsContainer from "./components/JobSetsContainer"

export interface JobSetsPageProps {
  groupJobsService: IGroupJobsService
  updateJobSetsService: UpdateJobSetsService
  autoRefreshMs: number | undefined
}

export const JobSetsPage = ({ groupJobsService, updateJobSetsService, autoRefreshMs }: JobSetsPageProps) => (
  <JobSetsContainer
    v2GroupJobsService={groupJobsService}
    v2UpdateJobSetsService={updateJobSetsService}
    jobSetsAutoRefreshMs={autoRefreshMs}
  />
)
