import JobSetsContainer from "./components/JobSetsContainer"

export interface JobSetsPageProps {
  autoRefreshMs: number | undefined
}

export const JobSetsPage = ({ autoRefreshMs }: JobSetsPageProps) => (
  <JobSetsContainer jobSetsAutoRefreshMs={autoRefreshMs} />
)
