import _ from "lodash"
import { useParams, Navigate } from "react-router-dom"

import { JOBS } from "../pathnames"
import {
  JobsTablePreferences,
  DEFAULT_PREFERENCES,
  stringifyQueryParams,
  toQueryStringSafe,
  QueryStringPrefs,
} from "../services/lookout/JobsTablePreferencesService"
import { StandardColumnId } from "../utils/jobsTableColumns"

const queryStringPrefsKeys: Array<keyof QueryStringPrefs> = ["f", "sb"]

// Redirects to the jobs table page with the job ID filter set to the path param, and the sidebar open to the job with this ID
export const JobIdRedirect = () => {
  const { jobId } = useParams()
  const jobsTablePreferences: Partial<JobsTablePreferences> = {
    filters: [{ id: StandardColumnId.JobID, value: jobId }],
    sidebarJobId: jobId,
  }
  const queryParams = _.pick(
    toQueryStringSafe({ ...DEFAULT_PREFERENCES, ...jobsTablePreferences }),
    queryStringPrefsKeys,
  )
  return <Navigate to={{ pathname: JOBS, search: `?${stringifyQueryParams(queryParams)}` }} />
}
