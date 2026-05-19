import { CommandSpec } from "../../config"

import { JobsTableContainer } from "./components/JobsTableContainer"

export interface JobsPageProps {
  debug: boolean
  autoRefreshMs: number | undefined
  commandSpecs: CommandSpec[]
}

export const JobsPage = ({ debug, autoRefreshMs, commandSpecs }: JobsPageProps) => (
  <JobsTableContainer debug={debug} autoRefreshMs={autoRefreshMs} commandSpecs={commandSpecs} />
)
