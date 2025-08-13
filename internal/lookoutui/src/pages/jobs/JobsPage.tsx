import { CommandSpec } from "../../config"
import { IGetJobsService } from "../../services/lookout/GetJobsService"
import { IGroupJobsService } from "../../services/lookout/GroupJobsService"
import { UpdateJobsService } from "../../services/lookout/UpdateJobsService"

import { JobsTableContainer } from "./components/JobsTableContainer"

export interface JobsPageProps {
  getJobsService: IGetJobsService
  groupJobsService: IGroupJobsService
  updateJobsService: UpdateJobsService
  debug: boolean
  autoRefreshMs: number | undefined
  commandSpecs: CommandSpec[]
}

export const JobsPage = ({
  getJobsService,
  groupJobsService,
  updateJobsService,
  debug,
  autoRefreshMs,
  commandSpecs,
}: JobsPageProps) => (
  <JobsTableContainer
    getJobsService={getJobsService}
    groupJobsService={groupJobsService}
    updateJobsService={updateJobsService}
    debug={debug}
    autoRefreshMs={autoRefreshMs}
    commandSpecs={commandSpecs}
  />
)
