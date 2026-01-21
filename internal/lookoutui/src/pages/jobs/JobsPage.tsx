import { CommandSpec } from "../../config"
import { IGroupJobsService } from "../../services/lookout/GroupJobsService"

import { JobsTableContainer } from "./components/JobsTableContainer"

export interface JobsPageProps {
  groupJobsService: IGroupJobsService
  debug: boolean
  autoRefreshMs: number | undefined
  commandSpecs: CommandSpec[]
}

export const JobsPage = ({ groupJobsService, debug, autoRefreshMs, commandSpecs }: JobsPageProps) => (
  <JobsTableContainer
    groupJobsService={groupJobsService}
    debug={debug}
    autoRefreshMs={autoRefreshMs}
    commandSpecs={commandSpecs}
  />
)
